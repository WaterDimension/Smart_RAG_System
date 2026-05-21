package com.yizhaoqi.smartpai.consumer;

import com.yizhaoqi.smartpai.config.KafkaConfig;
import com.yizhaoqi.smartpai.model.FileProcessingTask;
import com.yizhaoqi.smartpai.service.DocumentService;
import com.yizhaoqi.smartpai.service.ParseService;
import com.yizhaoqi.smartpai.service.VectorizationService;
import io.minio.errors.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * 文件处理消费者类
 * 监听 Kafka 中的文件处理任务消息，执行文件下载、解析、向量化等处理流程，并更新文档状态
 */
@Service
@Slf4j
public class FileProcessingConsumer {

    // 解析服务，用于提取文件中的文本内容并进行分块处理
    private final ParseService parseService;
    // 向量化服务，用于生成文本的向量表示（Embedding）并存储到 Elasticsearch 中
    private final VectorizationService vectorizationService;
    // 文档服务，用于更新文档的处理状态和重建索引等操作
    private final DocumentService documentService;
    @Autowired
    private KafkaConfig kafkaConfig;


    public FileProcessingConsumer(
            ParseService parseService,
            VectorizationService vectorizationService,
            DocumentService documentService
    ) {
        this.parseService = parseService;
        this.vectorizationService = vectorizationService;
        this.documentService = documentService;
    }

    /**
     * Producer 把 FileProcessingTask 对象 → JSON 字符串 → 字节，Consumer 把字节 → JSON 字符串 → FileProcessingTask 对象
     *
     *  @KafkaListener 监听, 订阅主题，并指定消费者组 ID  (subscribe + poll 轮询，只需要写处理方法)
     */
    @KafkaListener(topics = "#{kafkaConfig.getFileProcessingTopic()}",   // SpEL 表达式#{}动态解析主题名
    groupId = "#{kafkaConfig.getFileProcessingGroupId()}")
    public void processTask(FileProcessingTask task) {       //反序列化,方法参数直接拿到对象
        log.info("Received task: {}", task);
        log.info("文件权限信息: userId={}, orgTag={}, isPublic={}", 
                task.getUserId(), task.getOrgTag(), task.isPublic());

        // 1.标记为处理中，避免重复处理
        documentService.markVectorizationProcessing(task.getFileMd5(), false);

        // 判断任务类型: 如果是重建索引任务，直接调用重建索引方法，不需要下载和解析文件
        // NOTE
        if (FileProcessingTask.TASK_TYPE_REINDEX.equals(task.getTaskType())) {
            processReindexTask(task);
            return;  // 重建索引走不同路径
        }

        InputStream fileStream = null;
        try {
        // 2.下载 MinIO 中的合并文件(本地/远程http/https)，获取输入流
            fileStream = downloadFileFromStorage(task.getFilePath());
            // 在 downloadFileFromStorage 返回后立即检查流是否可读
            if (fileStream == null) {
                throw new IOException("流为空");
            }

            // 强制转换为可缓存流, 包装后，支持 mark/reset，解析器可以在需要时回退流位置，而不必重新下载文件
            if (!fileStream.markSupported()) {
                fileStream = new BufferedInputStream(fileStream);
            }

        // 3.解析文件（提取文本 + 分块） 把解析后的chunkText存入数据库，关联fileMd5，供后续向量化使用
            parseService.parseAndSave(task.getFileMd5(), fileStream,   //流式文档解析入库，不需要一次性加载到内存
                    task.getUserId(), task.getOrgTag(), task.isPublic());
            log.info("文件解析完成，fileMd5: {}", task.getFileMd5());

        // 4.向量化处理（Embedding + 存入 ES）
            VectorizationService.VectorizationUsageResult vectorizationResult = vectorizationService.vectorizeWithUsage(
                    task.getFileMd5(),
                    task.getUserId(),
                    task.getOrgTag(),
                    task.isPublic(),
                    task.getUserId()
            );

            // 标记完成, 把用量信息（使用的tokens数量、chunk数量、模型版本等）存入数据库,方便后面统计成本
            documentService.markVectorizationCompleted(task.getFileMd5(), vectorizationResult);
            log.info("向量化完成，fileMd5: {}", task.getFileMd5());
        } catch (Exception e) {
            // TODO 第一次向量化中断后，parseAndSave() 的 saveChildChunks()提取文本还要执行一次，只追加不清理
            documentService.markVectorizationFailed(task.getFileMd5(), e);
            log.error("Error processing task: {}", task, e);
            // 抛出异常让 Kafka  DefaultErrorHandler捕获并触发重试 / 死信
            throw new RuntimeException("Error processing task", e);
        } finally {
            // 确保关闭输入流
            if (fileStream != null) {
                try {
                    fileStream.close();
                } catch (IOException e) {
                    log.error("Error closing file stream", e);
                }
            }
        }
    }

    private void processReindexTask(FileProcessingTask task) {
        try {
            String requesterId = task.getRequesterId() == null || task.getRequesterId().isBlank()
                    ? task.getUserId()
                    : task.getRequesterId();
            documentService.reindexDocument(task.getFileMd5(), requesterId);
        } catch (Exception e) {
            documentService.markVectorizationFailed(task.getFileMd5(), e);
            log.error("Error reindexing task: {}", task, e);
            throw new RuntimeException("Error reindexing task", e);
        }
    }

    /**
     * 从存储系统下载文件
     *
     * @param filePath 文件路径或 URL
     * @return 文件输入流
     */
    private InputStream downloadFileFromStorage(String filePath) throws
            ServerException, InsufficientDataException, ErrorResponseException, IOException,
            NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        log.info("Downloading file from storage: {}", filePath);

        try {
            // 如果是文件系统路径
            File file = new File(filePath);
            if (file.exists()) {
                log.info("Detected file system path: {}", filePath);
                return new FileInputStream(file);
            }

            // 如果是远程 URL
            if (filePath.startsWith("http://") || filePath.startsWith("https://")) {
                log.info("Detected remote URL: {}", filePath);
                URL url = new URL(filePath);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setConnectTimeout(30000); // 连接超时30秒
                connection.setReadTimeout(180000);   // 读取超时时间3分钟

                // 添加必要的请求头
                connection.setRequestProperty("User-Agent", "SmartPAI-FileProcessor/1.0");

                int responseCode = connection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    log.info("Successfully connected to URL, starting download...");
                    return connection.getInputStream();
                } else if (responseCode == HttpURLConnection.HTTP_FORBIDDEN) {
                    log.error("Access forbidden - possible expired presigned URL");
                    throw new IOException("Access forbidden - the presigned URL may have expired");
                } else {
                    log.error("Failed to download file, HTTP response code: {} for URL: {}", responseCode, filePath);
                    throw new IOException(String.format("Failed to download file, HTTP response code: %d", responseCode));
                }
            }

            // 如果既不是文件路径也不是 URL
            throw new IllegalArgumentException("Unsupported file path format: " + filePath);
        } catch (Exception e) {
            log.error("Error downloading file from storage: {}", filePath, e);
            return null; // 或者抛出异常
        }
    }

}
