package com.yizhaoqi.smartpai.service;

import com.yizhaoqi.smartpai.model.DocumentVector;
import com.yizhaoqi.smartpai.repository.DocumentVectorRepository;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.xml.sax.SAXException;

import java.io.*;
import java.text.Normalizer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.StandardTokenizer;

@Service
public class ParseService {

    private static final Logger logger = LoggerFactory.getLogger(ParseService.class);
    private static final int PDF_BOUNDARY_SCAN_LINES = 3;
    private static final int PDF_BOILERPLATE_MIN_LENGTH = 4;
    private static final int PDF_BOILERPLATE_MAX_LENGTH = 120;

    @Autowired
    private DocumentVectorRepository documentVectorRepository;

    @Autowired
    private UsageQuotaService usageQuotaService;

    @Value("${file.parsing.chunk-size}")
    private int chunkSize;   // 512字符的子切片大小，适合大模型的输入限制，同时保持足够的上下文信息

    @Value("${file.parsing.overlap-size:100}")
    private int overlapSize = 100; // 默认重叠100字符，保证上下文连贯，避免切分点断句导致AI理解困难

    @Value("${file.parsing.min-chunk-size:100}")
    private int minChunkSize = 100;

    @Value("${file.parsing.parent-chunk-size:1048576}")
    private int parentChunkSize;
    
    @Value("${file.parsing.buffer-size:8192}")
    private int bufferSize;
    
    @Value("${file.parsing.max-memory-threshold:0.8}")
    private double maxMemoryThreshold;
    
    public ParseService() {
        // 无需初始化，StandardTokenizer是静态方法
    }

    /**
     * 以流式方式解析文件，将内容分块并保存到数据库，以避免OOM。
     * 采用"父文档-子切片"策略。
     *
     * @param fileMd5    文件的MD5哈希值，用于唯一标识文件
     * @param fileStream 文件输入流，用于读取文件内容
     * @param userId     上传用户ID
     * @param orgTag     组织标签
     * @param isPublic   是否公开
     * @throws IOException   如果文件读取过程中发生错误
     * @throws TikaException 如果文件解析过程中发生错误
     */
    public void parseAndSave(String fileMd5, InputStream fileStream,
            String userId, String orgTag, boolean isPublic) throws IOException, TikaException {
        logger.info("开始流式解析文件，fileMd5: {}, userId: {}, orgTag: {}, isPublic: {}",
                fileMd5, userId, orgTag, isPublic);

        // 在解析前检查内存使用情况，防止解析大文件时内存不足导致程序崩溃
        checkMemoryThreshold();

        // BufferedInputStream 内部实现了 mark/reset, 可以让我们在读取文件头部进行类型嗅探后，重置流位置，而不需要重新打开文件流
        try (BufferedInputStream bufferedStream = new BufferedInputStream(fileStream, bufferSize)) {
            /**
             *  PDF 专用路径，按页解析(PDF 有天然的页面边界, 好做页切分)
             *
             *  Apache PDFBox 按页解析路径, 不用Tika了, 直接用PDFBox的API按页提取文本, 这样更快更干净
             */
            // 嗅探文件类型 → 需要读前几个字节来判断是否是PDF，PDF文件有明显的"%PDF-"头部标识
            if (isPdfDocument(bufferedStream)) {
                parsePdfAndSave(fileMd5, bufferedStream, userId, orgTag, isPublic);
                logger.info("PDF 文件页级解析和入库完成，fileMd5: {}", fileMd5);
                return;
            }
            /**
             *  普通文件：Tika + 自定义 StreamingContentHandler 流式处理
             *
             *  创建一个流式处理器，实现 SAX 的 ContentHandler 接口，在 characters() 里累积文本
             *  当累积的文本达到 parentChunkSize 阈值， handler 内部自动触发：切父块 → 分子切片 → 写入数据库
             *  这样整个文件不需要一次性全部加载到内存，流式处理，避免 OOM
             */
            StreamingContentHandler handler = new StreamingContentHandler(fileMd5, userId, orgTag, isPublic);
            Metadata metadata = new Metadata();  // Tika解析需要的元数据对象，虽然我们不需要特别的元数据，但这个对象必须传入解析器
            ParseContext context = new ParseContext();  // Tika解析需要的上下文对象，虽然我们不需要特别的上下文配置，但这个对象必须传入解析器
            AutoDetectParser parser = new AutoDetectParser(); // Tika的万能解析器，能自动识别文件类型并提取文本内容

            // Tika的parse方法会驱动整个流式处理过程
            // 当handler的characters方法接收到足够数据时，会触发分块、切片和保存
            parser.parse(bufferedStream, handler, metadata, context);

            logger.info("文件流式解析和入库完成，fileMd5: {}", fileMd5);

        } catch (SAXException e) {
            logger.error("文档解析失败，fileMd5: {}", fileMd5, e);
            throw new RuntimeException("文档解析失败", e);
        }
    }

    /**
     * 兼容旧版本的解析方法,
     * 传入文件流，返回【预估Token数 + 预估分片数】
     */
    public void parseAndSave(String fileMd5, InputStream fileStream) throws IOException, TikaException {
        // 使用默认值调用新方法
        parseAndSave(fileMd5, fileStream, "unknown", "DEFAULT", false);
    }

    /**
     *  传入文件流，返回【预估Token数 + 文本分片数】
     */
    public EmbeddingEstimate estimateEmbeddingUsage(InputStream fileStream) throws IOException, TikaException {
        logger.info("开始估算文档 Embedding Token");
        // 1.先检查内存是否足够，防止解析时卡死
        checkMemoryThreshold();

        // 2. 把文件流包装成【缓冲流】→ 读文件更快
        try (BufferedInputStream bufferedStream = new BufferedInputStream(fileStream, bufferSize)) {
            // 3. 如果是PDF文件 → 走专门的PDF解析逻辑更快
            if (isPdfDocument(bufferedStream)) {
                return estimatePdfEmbeddingUsage(bufferedStream);
            }
            // 4.自定义计算器:StreamingEstimateHandler，边读文本边算Token
            StreamingEstimateHandler handler = new StreamingEstimateHandler();
            // Tika解析需要的固定参数
            Metadata metadata = new Metadata();
            ParseContext context = new ParseContext();

            //Tika万能解析器：自动识别文件类型，边解析边把文本丢给我们写的StreamingEstimateHandler
            AutoDetectParser parser = new AutoDetectParser();

            //核心执行：解析文件文本 → 实时传给handler计算Token → 最后从handler拿到总Token数和总分片数
            parser.parse(bufferedStream, handler, metadata, context);
            return handler.snapshot();
        } catch (SAXException e) {
            logger.error("文档 Embedding Token 估算失败", e);
            throw new RuntimeException("文档 Embedding Token 估算失败", e);
        }
    }

    // 检查服务器内存是否充足，防止解析大文件把程序搞崩
    private void checkMemoryThreshold() {
        // 1. 获取Java程序的内存管理器（掌管所有内存的工具）
        Runtime runtime = Runtime.getRuntime();

        // 2. 读取内存的4个核心数据
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;

        // 3. 计算【内存使用率】
        double memoryUsage = (double) usedMemory / maxMemory;

        // 4. 如果内存使用率超过了警戒线0.8
        if (memoryUsage > maxMemoryThreshold) {
            logger.warn("内存使用率过高: {:.2f}%, 触发垃圾回收", memoryUsage * 100);
            // 主动触发【垃圾回收】：清理程序里没用的对象，腾出内存
            System.gc();
            
            // 清理完内存，再试一次
            usedMemory = runtime.totalMemory() - runtime.freeMemory();
            memoryUsage = (double) usedMemory / maxMemory;
            
            if (memoryUsage > maxMemoryThreshold) {
                throw new RuntimeException("内存不足，无法处理大文件。当前使用率: " + 
                    String.format("%.2f%%", memoryUsage * 100));
            }
        }
    }
    
    /**
     * 内部流式内容处理器，实现了父子文档切分策略的核心逻辑。
     * Tika解析器会调用characters方法，当累积的文本达到"父块"大小时，
     * 就触发processParentChunk方法，进行"子切片"的生成和入库。
     */
    private class StreamingContentHandler extends BodyContentHandler {
        private final StringBuilder buffer = new StringBuilder();
        private final String fileMd5;
        private final String userId;
        private final String orgTag;
        private final boolean isPublic;
        private int savedChunkCount = 0;  // 这个计数器用来给子切片分配连续的chunkId，确保同一文件的切片ID是连续的，方便后续查询和调试

        // 父块层：1MB 缓冲区（仅内存，不持久化）   
        public StreamingContentHandler(String fileMd5, String userId, String orgTag, boolean isPublic) {
            super(-1); // 禁用Tika的内部写入限制，我们自己管理缓冲区
            this.fileMd5 = fileMd5;
            this.userId = userId;
            this.orgTag = orgTag;
            this.isPublic = isPublic;
        }

        @Override
        public void characters(char[] ch, int start, int length) {
            buffer.append(ch, start, length);
            // 每攒够 1MB 触发一次， 把缓冲区里的文字处理成子切片入库（父->子切片）
            if (buffer.length() >= parentChunkSize) {
                processParentChunk();
            }
        }

        @Override
        public void endDocument() {
            // 处理文档末尾剩余的最后一部分内容
            if (buffer.length() > 0) {
                processParentChunk();
            }
        }

        // 子块层：512字符检索单元（持久化到DB和ES）
        private void processParentChunk() {
            String parentChunkText = buffer.toString();
            logger.debug("处理父文本块，大小: {} bytes", parentChunkText.length());

            // 1. 将父块分割成更小的、有语义的子切片
            List<String> childChunks = ParseService.this.splitTextIntoChunksWithSemantics(parentChunkText, chunkSize);

            // 2. 将子切片批量保存到数据库, 返回最新的总分片数，继续给下一个父块分配chunkId
            this.savedChunkCount = ParseService.this.saveChildChunks(
                    fileMd5, childChunks, userId, orgTag, isPublic, this.savedChunkCount, null
            );

            // 3. 清空缓冲区，为下一个父块做准备
            buffer.setLength(0);
        }
    }

    // 自定义的【文件文字计算器】，专门接收Tika提取的文字，实时算Token
    private class StreamingEstimateHandler extends BodyContentHandler {

        // 1. 临时缓冲区：用来积累Tika传来的文本，直到达到父块大小就切分成子块
        private final StringBuilder buffer = new StringBuilder();
        // 2. 累计总Token数（整个文件的预估Token）
        private long estimatedTokens = 0L;
        // 3. 累计总分片数（整个文件要切多少段给AI）
        private int estimatedChunkCount = 0;

        // 构造方法：super(-1) = 不限制文字长度，大文件也能处理
        private StreamingEstimateHandler() {
            super(-1);
        }

        // Tika 提取到一段文字，就自动跑这里：把文字放进临时buffer
        @Override
        public void characters(char[] ch, int start, int length) {
            buffer.append(ch, start, length);
            // buffer装满（达到设定父块大小），立即计算token，然后清空buffer继续装(存满一批算一次：性能最大化)
            if (buffer.length() >= parentChunkSize) {
                processParentChunk();
            }
        }

        // 文件全部读完了，执行这个方法
        @Override
        public void endDocument() {
            // 文件读完了，buffer里如果还有剩下的文字，必须处理完！
            if (buffer.length() > 0) {
                processParentChunk();
            }
        }

        //真正的【计算逻辑】
        private void processParentChunk() {
            // 1. 把临时篮子里的文字，按：段落 + 语义智能分片（不硬切句子，保证AI能看懂）
            List<String> childChunks = ParseService.this.splitTextIntoChunksWithSemantics(buffer.toString(), chunkSize);
            // 2. 累加：本次切了多少段 → 总片数 += 这个数字
            estimatedChunkCount += childChunks.size();

            // 3. 调用工具：计算这些片段的总Token数 → 总Token += 这个数字
            estimatedTokens += usageQuotaService.estimateEmbeddingTokens(childChunks);

            //清空， 继续装下一批文字
            buffer.setLength(0);
        }
        // 把算好的 总Token、总分片数 打包成结果返回
        private EmbeddingEstimate snapshot() {
            return new EmbeddingEstimate(estimatedTokens, estimatedChunkCount);
        }
    }

    /**
     * 将子切片列表保存到数据库。
     *
     * @param fileMd5         文件的 MD5 哈希值
     * @param chunks          子切片文本列表
     * @param userId          上传用户ID
     * @param orgTag          组织标签
     * @param isPublic        是否公开
     * @param startingChunkId 当前批次的起始分片ID
     * @return 保存后总的分片数量
     */
    private int saveChildChunks(String fileMd5, List<String> chunks,
            String userId, String orgTag, boolean isPublic, int startingChunkId, Integer pageNumber) {
        int currentChunkId = startingChunkId;
        for (String chunk : chunks) {
            currentChunkId++;   // 每保存一个子切片，chunkId自增1，确保同一文件的切片ID是连续的，方便后续查询和调试
            var vector = new DocumentVector();
            vector.setFileMd5(fileMd5);
            vector.setChunkId(currentChunkId);
            vector.setTextContent(chunk);
            vector.setPageNumber(pageNumber);
            vector.setAnchorText(buildAnchorText(chunk));
            vector.setUserId(userId);
            vector.setOrgTag(orgTag);
            vector.setPublic(isPublic);
            documentVectorRepository.save(vector);
        }
        logger.info("成功保存 {} 个子切片到数据库", chunks.size());
        return currentChunkId;  // 返回最新的总分片数，供下一批父块继续分配chunkId
    }

    /**
     * 专门处理PDF文件的解析和保存逻辑，按页提取文本，清理页眉页脚，智能分块，并保存到数据库。
     * @param fileMd5
     * @param fileStream
     * @param userId
     * @param orgTag
     * @param isPublic
     * @throws IOException
     */
    private void parsePdfAndSave(String fileMd5, InputStream fileStream, String userId, String orgTag, boolean isPublic) throws IOException {
        // 把 PDF 加载到内存
        try (PDDocument document = PDDocument.load(fileStream)) {
            int savedChunkCount = 0;

            //  extractCleanPdfPageTexts提取每页的纯文本（会做清洗）
            List<String> cleanedPageTexts = extractCleanPdfPageTexts(document);

            // 遍历每一页的文本，进行智能分块和入库
            for (int pageNumber = 1; pageNumber <= cleanedPageTexts.size(); pageNumber++) {
                String pageText = cleanedPageTexts.get(pageNumber - 1);
                if (pageText == null || pageText.isBlank()) {
                    continue;
                }

                //  对每一页的文本，调用 splitTextIntoChunksWithSemantics() 做语义分块，得到子切片列表
                List<String> childChunks = splitTextIntoChunksWithSemantics(pageText, chunkSize);

                //  把子切片ChunkText写入数据库，pageNumber 作为元数据一起存储，方便后续查询
                savedChunkCount = saveChildChunks(fileMd5, childChunks, userId, orgTag, isPublic, savedChunkCount, pageNumber);
            }
        }
    }

    private EmbeddingEstimate estimatePdfEmbeddingUsage(InputStream fileStream) throws IOException {
        try (PDDocument document = PDDocument.load(fileStream)) {
            long estimatedTokens = 0L;
            int estimatedChunkCount = 0;

            List<String> cleanedPageTexts = extractCleanPdfPageTexts(document);
            for (String pageText : cleanedPageTexts) {
                if (pageText == null || pageText.isBlank()) {
                    continue;
                }

                List<String> childChunks = splitTextIntoChunksWithSemantics(pageText, chunkSize);
                estimatedChunkCount += childChunks.size();
                estimatedTokens += usageQuotaService.estimateEmbeddingTokens(childChunks);
            }

            return new EmbeddingEstimate(estimatedTokens, estimatedChunkCount);
        }
    }

    /**
     * 提取PDF每页的文本，并清理掉重复出现的页眉页脚等无意义内容，返回清洗后的每页文本列表。
      1. 使用 PDFTextStripper 按页提取原始文本
      2. 收集每页顶部和底部的前几行文本，统计它们在整个文档中出现的频率
      3. 根据频率判断哪些行是页眉页脚（出现超过阈值的行），并在每页文本中去除这些行
     * @param document
     * @return
     * @throws IOException
     */
    private List<String> extractCleanPdfPageTexts(PDDocument document) throws IOException {
        PDFTextStripper stripper = new PDFTextStripper();
        List<List<String>> rawPageLines = new ArrayList<>();

        for (int pageNumber = 1; pageNumber <= document.getNumberOfPages(); pageNumber++) {
            stripper.setStartPage(pageNumber);
            stripper.setEndPage(pageNumber);
            String pageText = stripper.getText(document);
            rawPageLines.add(splitPdfLines(pageText));
        }

        Map<String, Integer> topLineCounts = collectBoundaryLineCounts(rawPageLines, true);
        Map<String, Integer> bottomLineCounts = collectBoundaryLineCounts(rawPageLines, false);
        int repeatedThreshold = Math.max(2, Math.min(3, document.getNumberOfPages()));

        List<String> cleanedPages = new ArrayList<>(rawPageLines.size());
        for (int pageIndex = 0; pageIndex < rawPageLines.size(); pageIndex++) {
            List<String> cleanedLines = removePdfBoilerplateLines(
                    rawPageLines.get(pageIndex),
                    topLineCounts,
                    bottomLineCounts,
                    repeatedThreshold
            );
            String cleanedText = String.join("\n", cleanedLines).trim();
            cleanedPages.add(cleanedText);
        }

        return cleanedPages;
    }

    private List<String> splitPdfLines(String pageText) {
        if (pageText == null || pageText.isBlank()) {
            return new ArrayList<>();
        }

        String[] lines = pageText.split("\\R");
        List<String> result = new ArrayList<>(lines.length);
        for (String line : lines) {
            result.add(line == null ? "" : line.strip());
        }
        return result;
    }

    private Map<String, Integer> collectBoundaryLineCounts(List<List<String>> pageLines, boolean topBoundary) {
        Map<String, Integer> counts = new HashMap<>();

        for (List<String> lines : pageLines) {
            List<String> boundaryLines = topBoundary
                    ? firstMeaningfulLines(lines, PDF_BOUNDARY_SCAN_LINES)
                    : lastMeaningfulLines(lines, PDF_BOUNDARY_SCAN_LINES);

            for (String line : boundaryLines) {
                String key = normalizePdfBoundaryLine(line);
                if (key == null) {
                    continue;
                }
                counts.merge(key, 1, Integer::sum);
            }
        }

        return counts;
    }

    private List<String> firstMeaningfulLines(List<String> lines, int maxCount) {
        List<String> result = new ArrayList<>(maxCount);
        for (String line : lines) {
            if (line == null || line.isBlank()) {
                continue;
            }
            result.add(line);
            if (result.size() >= maxCount) {
                break;
            }
        }
        return result;
    }

    private List<String> lastMeaningfulLines(List<String> lines, int maxCount) {
        List<String> result = new ArrayList<>(maxCount);
        for (int index = lines.size() - 1; index >= 0; index--) {
            String line = lines.get(index);
            if (line == null || line.isBlank()) {
                continue;
            }
            result.add(0, line);
            if (result.size() >= maxCount) {
                break;
            }
        }
        return result;
    }

    private List<String> removePdfBoilerplateLines(
            List<String> lines,
            Map<String, Integer> topLineCounts,
            Map<String, Integer> bottomLineCounts,
            int repeatedThreshold) {

        int start = 0;
        int remainingTopChecks = PDF_BOUNDARY_SCAN_LINES;
        while (start < lines.size() && remainingTopChecks > 0) {
            String line = lines.get(start);
            if (line == null || line.isBlank()) {
                start++;
                continue;
            }

            String key = normalizePdfBoundaryLine(line);
            if (key == null || topLineCounts.getOrDefault(key, 0) < repeatedThreshold) {
                break;
            }

            logger.debug("过滤 PDF 页眉文本: {}", line);
            start++;
            remainingTopChecks--;
        }

        int end = lines.size() - 1;
        int remainingBottomChecks = PDF_BOUNDARY_SCAN_LINES;
        while (end >= start && remainingBottomChecks > 0) {
            String line = lines.get(end);
            if (line == null || line.isBlank()) {
                end--;
                continue;
            }

            String key = normalizePdfBoundaryLine(line);
            if (key == null || bottomLineCounts.getOrDefault(key, 0) < repeatedThreshold) {
                break;
            }

            logger.debug("过滤 PDF 页脚文本: {}", line);
            end--;
            remainingBottomChecks--;
        }

        List<String> cleanedLines = new ArrayList<>();
        for (int index = start; index <= end; index++) {
            cleanedLines.add(lines.get(index));
        }
        return cleanedLines;
    }

    private String normalizePdfBoundaryLine(String line) {
        if (line == null) {
            return null;
        }

        String normalized = Normalizer.normalize(line, Normalizer.Form.NFKC)
                .replace('\u00A0', ' ')
                .replaceAll("\\s+", " ")
                .replaceAll("\\d+", "#")
                .trim()
                .toLowerCase(Locale.ROOT);

        if (normalized.length() < PDF_BOILERPLATE_MIN_LENGTH || normalized.length() > PDF_BOILERPLATE_MAX_LENGTH) {
            return null;
        }

        return normalized;
    }

    private boolean isPdfDocument(BufferedInputStream stream) throws IOException {
        stream.mark(bufferSize);
        byte[] header = stream.readNBytes(5);
        stream.reset();
        return header.length == 5 && "%PDF-".equals(new String(header, StandardCharsets.US_ASCII));
    }
/**
 *  构建锚文本：从文本块中提取一个简短的摘要，作为这个块的代表，方便后续展示和调试。
 *  1. 取文本块的前120个字符，去掉多余的空白，作为锚文本
 *  2. 如果文本块过短（少于10字符），就不生成锚文本，返回null
 *  3. 如果文本块过长，截断后加省略号，保持锚文本简洁
 */
    private String buildAnchorText(String chunk) {
        if (chunk == null || chunk.isBlank()) {
            return null;
        }

        String normalized = chunk.replaceAll("\\s+", " ").trim();
        int maxLength = 120;
        if (normalized.length() <= maxLength) {
            return normalized;
        }
        return normalized.substring(0, maxLength) + "…";
    }

    /**
     * 智能文本分割，保持语义完整性
     *
     *   1. 段落边界 — 按 \n\n+ 分割
     *   2. 句子边界 — 按中英文标点（。！？；.!?;）切分长段落
     *   3. HanLP 分词边界 — 对超长句子使用 StandardTokenizer 按词切分
     *   4. HanLP挂了->字符级回退 — 单字符切分作为兜底
     */
    private List<String> splitTextIntoChunksWithSemantics(String text, int chunkSize) {
        if (text == null || text.isBlank()) {
            return new ArrayList<>();
        }

        int effectiveChunkSize = Math.max(1, chunkSize);   //一般都是512，1只是为防止除0错误
        // 第一步：按段落边界切分，得到初步的文本块列表
        List<String> baseChunks = splitTextIntoBaseChunks(text, effectiveChunkSize);
        // 第二步：合并过小的文本块，避免产生大量无意义的碎片
        List<String> mergedChunks = mergeSmallChunks(baseChunks, effectiveChunkSize);
        // 第三步：在文本块之间添加语义重叠，增强上下文连贯性
        return addSemanticOverlap(mergedChunks, effectiveChunkSize);
    }

    // 按段落边界切分，得到初步的文本块列表
    private List<String> splitTextIntoBaseChunks(String text, int chunkSize) {
        // 装分割结果
        List<String> chunks = new ArrayList<>();

        // 按段落分割
        String[] paragraphs = text.split("\n\n+");

        //箱子，装段落的，装满了就丢到chunks里，重新开一个箱子
        StringBuilder currentChunk = new StringBuilder();

        // 每个段落
        for (String paragraph : paragraphs) {
            if (paragraph == null || paragraph.isBlank()) {
                continue;
            }

            paragraph = paragraph.trim();

            // 1.单段落超过chunk大小，需要进一步分割
            if (paragraph.length() > chunkSize) {
                // 里面有其他段落，先封箱
                if (currentChunk.length() > 0) {
                    chunks.add(currentChunk.toString().trim());
                    currentChunk = new StringBuilder();
                }

                //  调用splitLongParagraph把这个长段落按句子切开，得到一个句子列表
                List<String> sentenceChunks = splitLongParagraph(paragraph, chunkSize);
                chunks.addAll(sentenceChunks);
            }
            // 当前箱子装不下
            else if (currentChunk.length() + paragraph.length() + paragraphSeparatorLength(currentChunk) > chunkSize) {
                // 保存当前chunk
                if (currentChunk.length() > 0) {
                    chunks.add(currentChunk.toString().trim());
                }
                // 新开一个chunk装这个段落;
                currentChunk = new StringBuilder(paragraph);
            }
            // 段落追加到当前chunk;
            else {
                if (currentChunk.length() > 0) {
                    currentChunk.append("\n\n");
                }
                currentChunk.append(paragraph);
            }
        }

        // 添加最后一个chunk
        if (currentChunk.length() > 0) {
            chunks.add(currentChunk.toString().trim());
        }

        return chunks;
    }

    private int paragraphSeparatorLength(StringBuilder currentChunk) {
        return currentChunk.length() > 0 ? 2 : 0;
    }


    // 把小碎片合并成大块
    private List<String> mergeSmallChunks(List<String> chunks, int chunkSize) {
        List<String> merged = new ArrayList<>();
        // 合并后最小块大小 = minChunkSize（默认100）和chunkSize 512的较小值，保证合并条件合理，避免过度合并导致块过大 (定义碎片大小的上限)
        int effectiveMinChunkSize = normalizedMinChunkSize(chunkSize);
        // 合并后最大块大小 = chunkSize + overlapSize做缓冲（允许合并后稍微超过chunkSize，但不能太离谱，保持在chunkSize + 100字符以内）
        int maxMergedChunkSize = chunkSize + normalizedOverlapSize(chunkSize);  // 合并上限

        for (String chunk : chunks) {
            // 1. 把当前块chunk标准化->normalizedChunk（去掉多余空白）
            String normalizedChunk = normalizeChunk(chunk);
            // 无信息的块
            if (normalizedChunk.isEmpty()) {
                continue;
            }

            // 第一个后的块
            if (!merged.isEmpty()) {
                // 2. 取出前一个块
                String previous = merged.get(merged.size() - 1);
                // 3. 试着把前一个块和当前块拼起来
                String combined = combineChunks(previous, normalizedChunk);

                // 4. 判断要不要合并(条件：前一个块或当前块有一个是小碎片，并且合并后不超过最大合并块大小)
                if ((normalizedChunk.length() < effectiveMinChunkSize || previous.length() < effectiveMinChunkSize)
                        && combined.length() <= maxMergedChunkSize) {
                    // 5. 合并：替换掉前一个块
                    merged.set(merged.size() - 1, combined);
                    // 6. 不符合合并条件，当前块独立加入
                    continue;
                }
            }

            merged.add(normalizedChunk);
        }

        return merged;
    }

    private String normalizeChunk(String chunk) {
        return chunk == null ? "" : chunk.trim();
    }

    private int normalizedMinChunkSize(int chunkSize) {
        if (minChunkSize <= 0) {
            return 0;
        }
        return Math.min(minChunkSize, chunkSize);
    }

    /**
     * 根据 chunkSize 规范化 overlapSize，确保重叠长度合理，既能增强上下文连贯性，又不会导致块过大。
      1. 如果 overlapSize <= 0 或 chunkSize <= 1，说明不需要重叠，直接返回0
      2. 否则，重叠长度不能超过 chunkSize - 1（至少要留一个字符给当前块），也不能超过默认的 overlapSize（100），取两者的较小值
     * @param chunkSize
     * @return
     */
    private int normalizedOverlapSize(int chunkSize) {
        if (overlapSize <= 0 || chunkSize <= 1) {
            return 0;
        }
        return Math.min(overlapSize, chunkSize - 1);
    }

    private String combineChunks(String first, String second) {
        if (first == null || first.isBlank()) {
            return normalizeChunk(second);
        }
        if (second == null || second.isBlank()) {
            return normalizeChunk(first);
        }
        return normalizeChunk(first) + "\n\n" + normalizeChunk(second);
    }

    /**
     * 在相邻块之间添加语义重叠，增强上下文连贯性
      1. 计算合理的重叠长度（不能超过 chunkSize - 1，也不能超过默认的 overlapSize 100）
      2. 如果重叠长度不合理，或者块数量太少，就不添加重叠了，直接返回原块列表
      3. 否则，遍历块列表，从第二块开始，在每块前面添加一个重叠文本，这个重叠文本是从前一个块的末尾提取的，长度不超过 overlapSize 的文本，保持语义完整
          - 提取重叠文本的逻辑: 让每个 chunk 都"看到"前一个 chunk 的结尾部分
              1. 从上一个chunk的末尾倒着取句子
              2. 累加这些句子，总长度不超过 overlapSize(100)
              3. 遇到单个句子超过100字的，用 HanLP 按词边界截取尾部
              4. HanLP 失败就用字符截取
     */
    // 在相邻块之间添加语义重叠，增强上下文连贯性
    private List<String> addSemanticOverlap(List<String> chunks, int chunkSize) {
        // 1. 相邻块之间共享的文本长度 ，不能超过 chunkSize - 1 (<512)，确保重叠合理 < overlapsize 100，既能增强上下文连贯性，又不会导致块过大
        int effectiveOverlapSize = normalizedOverlapSize(chunkSize);
        // 2. 如果重叠长度不合理，或者块数量太少，就不添加重叠了，直接返回原块列表
        if (effectiveOverlapSize <= 0 || chunks.size() <= 1) {
            return chunks;
        }

        /**
         * 提取重叠文本的逻辑: 让每个 chunk 都"看到"前一个 chunk 的结尾部分
         *   1. 从上一个chunk的末尾倒着取句子
         *   2. 累加这些句子，总长度不超过 overlapSize(100)
         *   3. 遇到单个句子超过100字的，用 HanLP 按词边界截取尾部
         *   4. HanLP 失败就用字符截取
         */
        List<String> overlappedChunks = new ArrayList<>(chunks.size());
        overlappedChunks.add(chunks.get(0));

        for (int i = 1; i < chunks.size(); i++) {
            // 从前一个块的末尾提取”语义“连贯的文本，长度不超过100字的文本
            String overlapText = buildOverlapText(chunks.get(i - 1), effectiveOverlapSize);
            String currentChunk = chunks.get(i);
            // 前一个块的末尾没有足够的文本做重叠
            if (overlapText.isEmpty()) {
                overlappedChunks.add(currentChunk);
            } else {
                // 合并前个块
                overlappedChunks.add(overlapText + "\n\n" + currentChunk);
            }
        }

        return overlappedChunks;
    }


    // 从文本末尾开始，按句子为单位向前收集，直到达到最大长度限制 ，确保重叠文本在语义边界处切分。
    private String buildOverlapText(String text, int maxLength) {
        if (text == null || text.isBlank() || maxLength <= 0) {
            return "";
        }

        // 1. 先把文本切成句子单元
        List<String> sentences = splitIntoSentenceUnits(text);
        StringBuilder overlap = new StringBuilder();

        // 2. 从后往前遍历 句子（从末尾开始收集）
        for (int i = sentences.size() - 1; i >= 0; i--) {
            String sentence = sentences.get(i).trim();
            if (sentence.isEmpty()) {
                continue;
            }
        // 情况A：单个句子超过maxLength 100
            if (sentence.length() > maxLength) {
                return overlap.isEmpty()
                        ? tailByTokenBoundary(sentence, maxLength)   // 还没收集到任何东西，用HanLP从尾部截(按词语边界切分)
                        : overlap.toString().trim();                // 已经有内容了，直接返回
            }
        // 情况B：句子本身正常，但加上它会超过 maxLength
            if (overlap.length() + sentence.length() > maxLength) {
                break;
            }
            // 情况C：加上这句还不超，插到前面
            overlap.insert(0, sentence);
        }

        if (overlap.isEmpty()) {
            // 所有句子都超长 （超过 maxLength）且还没收集到任何内容
            // 或者文本本身就没有可用的句子分割
            return tailByTokenBoundary(text, maxLength);
        }
        return overlap.toString().trim();
    }

    private List<String> splitIntoSentenceUnits(String text) {
        List<String> sentences = new ArrayList<>();
        Matcher matcher = Pattern.compile("[^。！？；.!?;]+[。！？；.!?;]?").matcher(text);
        while (matcher.find()) {
            String sentence = matcher.group().trim();
            if (!sentence.isEmpty()) {
                sentences.add(sentence);
            }
        }

        if (sentences.isEmpty()) {
            sentences.add(text.trim());
        }
        return sentences;
    }

    /**
     * 使用HanLP的StandardTokenizer从文本末尾按词边界截取，确保重叠文本在语义边界处切分，避免切到半个词导致AI理解困难。
      1. 如果文本本身就不超过maxLength，直接返回
      2. 否则，使用HanLP分词，从文本末尾开始累加词语，直到达到maxLength限制
      3. 如果HanLP分词失败了（可能是因为文本太长或有特殊字符），作为兜底方案，直接按字符从尾部截取maxLength长度的文本返回
     * @param text
     * @param maxLength
     * @return
     */
    private String tailByTokenBoundary(String text, int maxLength) {
        if (text == null || text.isBlank() || maxLength <= 0) {
            return "";
        }

        String normalized = text.trim();
        if (normalized.length() <= maxLength) {
            return normalized;
        }

        try {
            List<Term> termList = StandardTokenizer.segment(normalized);
            StringBuilder tail = new StringBuilder();
            for (int i = termList.size() - 1; i >= 0; i--) {
                String word = termList.get(i).word;
                if (word == null || word.isEmpty()) {
                    continue;
                }
                if (tail.length() + word.length() > maxLength) {
                    break;
                }
                tail.insert(0, word);
            }

            if (!tail.isEmpty()) {
                return tail.toString();
            }
        } catch (Exception e) {
            logger.debug("HanLP overlap 边界处理失败，使用字符兜底: {}", e.getMessage());
        }

        // 兜底：直接按字符从尾部截取maxLength长度的文本返回
        return normalized.substring(Math.max(0, normalized.length() - maxLength));
    }

    /**
     * 分割长段落，按句子边界
     */
    private List<String> splitLongParagraph(String paragraph, int chunkSize) {
        List<String> chunks = new ArrayList<>();

        // 按句子分割
        String[] sentences = paragraph.split("(?<=[。！？；])|(?<=[.!?;])\\s+");

        StringBuilder currentChunk = new StringBuilder();

        for (String sentence : sentences) {
            if (currentChunk.length() + sentence.length() > chunkSize) {
                if (currentChunk.length() > 0) {
                    chunks.add(currentChunk.toString().trim());
                    currentChunk = new StringBuilder();
                }

                // 如果单个句子太长，按词分割
                if (sentence.length() > chunkSize) {
                    chunks.addAll(splitLongSentence(sentence, chunkSize));
                } else {
                    currentChunk.append(sentence);
                }
            } else {
                currentChunk.append(sentence);
            }
        }

        if (currentChunk.length() > 0) {
            chunks.add(currentChunk.toString().trim());
        }

        return chunks;
    }

    /**
     * 使用HanLP智能分割超长句子，中文按语义切割
     */
    private List<String> splitLongSentence(String sentence, int chunkSize) {
        List<String> chunks = new ArrayList<>();
        
        try {
            // 使用HanLP StandardTokenizer进行分词
            List<Term> termList = StandardTokenizer.segment(sentence);
            
            StringBuilder currentChunk = new StringBuilder();
            for (Term term : termList) {
                String word = term.word;
                
                // 如果添加这个词会超过chunk大小限制，且当前chunk不为空
                if (currentChunk.length() + word.length() > chunkSize && !currentChunk.isEmpty()) {
                    chunks.add(currentChunk.toString());
                    currentChunk = new StringBuilder();
                }
                
                currentChunk.append(word);
            }
            
            if (!currentChunk.isEmpty()) {
                chunks.add(currentChunk.toString());
            }
            
            logger.debug("HanLP智能分词成功，原文长度: {}, 分词数: {}, 分块数: {}", 
                    sentence.length(), termList.size(), chunks.size());
                    
        } catch (Exception e) {
            logger.warn("HanLP分词异常: {}, 使用字符分割作为备用方案", e.getMessage());
            chunks = splitByCharacters(sentence, chunkSize);
         }
        
        return chunks;
    }
    
    /**
     * 备用方案：按字符分割
     */
    private List<String> splitByCharacters(String sentence, int chunkSize) {
        List<String> chunks = new ArrayList<>();
        StringBuilder currentChunk = new StringBuilder();

        for (int i = 0; i < sentence.length(); i++) {
            char c = sentence.charAt(i);

            if (currentChunk.length() + 1 > chunkSize && !currentChunk.isEmpty()) {
                chunks.add(currentChunk.toString());
                currentChunk = new StringBuilder();
            }

            currentChunk.append(c);
        }

        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk.toString());
        }

        return chunks;
    }

    // 结果记录：预估的Token数 + 预估的分片数
    public record EmbeddingEstimate(long estimatedTokens, int estimatedChunkCount) {
    }
}
