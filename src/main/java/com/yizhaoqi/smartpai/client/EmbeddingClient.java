package com.yizhaoqi.smartpai.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yizhaoqi.smartpai.service.ModelProviderConfigService;
import com.yizhaoqi.smartpai.service.RateLimitService;
import com.yizhaoqi.smartpai.service.UsageQuotaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.util.retry.Retry;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 嵌入向量生成客户端
@Component
public class EmbeddingClient {

    public enum UsageType {
        UPLOAD,
        QUERY
    }

    @Value("${embedding.api.batch-size:100}")
    private int batchSize;

    private static final Logger logger = LoggerFactory.getLogger(EmbeddingClient.class);
    private final ObjectMapper objectMapper;
    private final RateLimitService rateLimitService;
    private final UsageQuotaService usageQuotaService;
    private final ModelProviderConfigService modelProviderConfigService;

    public EmbeddingClient(ObjectMapper objectMapper,
                           RateLimitService rateLimitService,
                           UsageQuotaService usageQuotaService,
                           ModelProviderConfigService modelProviderConfigService) {
        this.objectMapper = objectMapper;
        this.rateLimitService = rateLimitService;
        this.usageQuotaService = usageQuotaService;
        this.modelProviderConfigService = modelProviderConfigService;
    }

    @PostConstruct
    public void init() {
        ModelProviderConfigService.ActiveProviderView provider = modelProviderConfigService.getActiveProvider(ModelProviderConfigService.SCOPE_EMBEDDING);
        logger.info("EmbeddingClient 初始化 - Provider: {}, 模型: {}, 批次大小: {}, 维度: {}, API地址: {}",
                provider.provider(), provider.model(), batchSize, provider.dimension(), provider.apiBaseUrl());
    }

    /**
     * 调用通义千问 API 生成向量
     * @param texts 输入文本列表
     * @return 对应的向量列表
     */
    public List<float[]> embed(List<String> texts) {
        return embedWithUsage(texts, "system", UsageType.UPLOAD).vectors();
    }

    public List<float[]> embed(List<String> texts, String requesterId) {
        return embedWithUsage(texts, requesterId, UsageType.UPLOAD).vectors();
    }

    // 这个方法主要用于查询时生成向量
    public List<float[]> embed(List<String> texts, String requesterId, UsageType usageType) {
        return embedWithUsage(texts, requesterId, usageType).vectors();
    }

    /**
     * 核心方法，包含了调用API、处理批次、记录使用量等逻辑
     * @param texts
     * @param requesterId       请求者ID（用于计费限流）
     * @param usageType        类型：UPLOAD（上传）或 QUERY（查询）
     * @return 包含向量列表和使用量信息的 查询向量结果对象
     */
    public EmbeddingUsageResult embedWithUsage(List<String> texts, String requesterId, UsageType usageType) {
        try {
            String normalizedRequesterId = requesterId == null || requesterId.isBlank() ? "unknown" : requesterId;
            logger.info("开始生成向量，文本数量: {}", texts.size());

            // 存储所有批次的向量结果和总Token用量
            List<float[]> all = new ArrayList<>(texts.size());
            int totalTokens = 0;

            // 1. 分批处理（每批 100 条)   实际我用的就1条
            for (int start = 0; start < texts.size(); start += batchSize) {
                int end = Math.min(start + batchSize, texts.size());
                List<String> sub = texts.subList(start, end);

                // 2. 限流 + 检查Token预算   QUERY类型扣费
                /**
                 *   每次预留成功后，会在内存中创建一个 TokenReservation 对象（redis预留记录）
                 *    TokenReservationBundle — 打包3条文本的Token预留信息（① 个人日额度、② 全网分钟预算、③ 全网日预算），
                 */
                UsageQuotaService.TokenReservationBundle reservation = usageType == UsageType.QUERY
                        ? rateLimitService.reserveEmbeddingQueryUsage(normalizedRequesterId, sub)   // 预留额度，后续根据实际消耗结算
                        : rateLimitService.reserveEmbeddingUploadUsage(normalizedRequesterId, sub);
                logger.debug("调用向量 API, 批次: {}-{} (size={})", start, end - 1, sub.size());
                try {
                    // 3. 调Embedding API生成向量, 得到JSON响应字符串
                    String response = callApiOnce(sub);

                    // 4. 解析响应（提取向量 + Token用量）         把 API 返回的 JSON 字符串解析成 Java 对象（向量列表 + Token 用量）
                    EmbeddingApiResponse parsedResponse = parseEmbeddingResponse(response, sub);

                    // 5. 结算额度（实际消耗 token）      把预扣的额度按实际消耗"多退少补"
                    usageQuotaService.settleReservation(reservation, parsedResponse.totalTokens());

                    // 6. 累加结果
                    all.addAll(parsedResponse.vectors());
                    totalTokens += parsedResponse.totalTokens();
                } catch (Exception e) {
                    // 7. 异常时释放预留额度
                    usageQuotaService.abortReservation(reservation);
                    throw e;
                }
            }
            logger.info("成功生成向量，总数量: {}", all.size());
            return new EmbeddingUsageResult(all, totalTokens, currentModelVersion());
        } catch (WebClientResponseException e) {
            // 提供详细的API响应错误信息
            logger.error("API调用失败 - 状态码: {}, 响应: {}, 请求头: {}",
                    e.getStatusCode().value(),
                    e.getResponseBodyAsString(),
                    e.getHeaders());
            throw new RuntimeException(String.format(
                    "向量生成失败 - API错误: HTTP %d - %s",
                    e.getStatusCode().value(),
                    e.getResponseBodyAsString()), e);
        } catch (Exception e) {
            logger.error("调用向量化 API 失败: {} - 类型: {}",
                    e.getMessage(),
                    e.getClass().getSimpleName(), e);
            throw new RuntimeException("向量生成失败: " + e.getMessage(), e);
        }
    }

    /**
     * 调用API生成向量的核心逻辑，包含重试机制
     * 向 Embedding API 发送 HTTP POST请求，获取向量的 JSON 字符串
     */
    private String callApiOnce(List<String> batch) {
        // 1.获取模型提供商配置
        ModelProviderConfigService.ActiveProviderView provider = modelProviderConfigService.getActiveProvider(ModelProviderConfigService.SCOPE_EMBEDDING);

        // 2.构造请求体
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("model", provider.model());
        requestBody.put("input", batch);           // 要向量化的文本
        if (provider.dimension() != null) {
            requestBody.put("dimensions", provider.dimension());
        }
        requestBody.put("encoding_format", "float");      // 向量以 float 格式返回

        logger.debug("发送嵌入请求 - Provider: {}, 模型: {}, 维度: {}, 批次大小: {}, 文本预览: {}",
                provider.provider(), provider.model(), provider.dimension(), batch.size(),
                batch.isEmpty() ? "空" : batch.get(0).substring(0, Math.min(50, batch.get(0).length())) + "...");

        // 3.构建 HTTP 客户端 → 发送 POST 请求
        return buildClient(provider).post()
                .uri("/embeddings")
                .bodyValue(requestBody)            // 请求体（自动转 JSON）
                .retrieve()                         // 发起请求
                .bodyToMono(String.class)
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1))   // 重试机制：最多重试3次，间隔1秒
                        .filter(e -> e instanceof WebClientResponseException)  // 仅对 HTTP 错误进行重试
                        .doBeforeRetry(signal -> logger.warn("重试API调用 - 尝试: {}, 错误: {}",
                                signal.totalRetries() + 1, signal.failure().getMessage())))
                .block(Duration.ofSeconds(30));                // 等待最多 30 秒
    }

    private WebClient buildClient(ModelProviderConfigService.ActiveProviderView provider) {
        WebClient.Builder builder = WebClient.builder()
                .baseUrl(ModelProviderConfigService.normalizeOpenAiCompatibleBaseUrl(provider.apiBaseUrl()))
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                // WebClient 的默认缓冲区大小限制（256KB）, 这里调高到 16MB
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024));
        if (provider.apiKey() != null && !provider.apiKey().isBlank()) {
            builder.defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + provider.apiKey());
        }
        return builder.build();
    }

    private EmbeddingApiResponse parseEmbeddingResponse(String response, List<String> inputTexts) throws Exception {
        // 1 把 JSON 字符串解析成 Jackson 的 JsonNode 树
        JsonNode jsonNode = objectMapper.readTree(response);
        // 2 提取 data 数组
        JsonNode data = jsonNode.get("data");  // 兼容模式下使用data字段
        if (data == null || !data.isArray()) {
            throw new RuntimeException("API 响应格式错误: data 字段不存在或不是数组");
        }
        // 3 遍历 data 数组，提取每个 embedding
        List<float[]> vectors = new ArrayList<>();
        for (JsonNode item : data) {
            JsonNode embedding = item.get("embedding");
            if (embedding != null && embedding.isArray()) {
                float[] vector = new float[embedding.size()];     // new float[2048]
                for (int i = 0; i < embedding.size(); i++) {
                    vector[i] = (float) embedding.get(i).asDouble();
                }
                vectors.add(vector);
            }
        }
        // 4 提取 usage 信息（Token 用量）
        JsonNode usage = jsonNode.path("usage");
        int totalTokens = usage.path("total_tokens").asInt(usage.path("input_tokens").asInt(0));
        // 5 如果 API 没有返回 token 数，用本地估算值兜底
        return new EmbeddingApiResponse(vectors, totalTokens > 0 ? totalTokens : usageQuotaService.estimateEmbeddingTokens(inputTexts));
    }

    public String currentModelVersion() {
        ModelProviderConfigService.ActiveProviderView provider = modelProviderConfigService.getActiveProvider(ModelProviderConfigService.SCOPE_EMBEDDING);
        return provider.provider() + ":" + provider.model() + ":" + provider.dimension();
    }

    private record EmbeddingApiResponse(List<float[]> vectors, int totalTokens) {
    }

    public record EmbeddingUsageResult(List<float[]> vectors, int totalTokens, String modelVersion) {
    }
}
