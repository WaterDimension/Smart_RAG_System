package com.yizhaoqi.smartpai.service;

import com.yizhaoqi.smartpai.config.RateLimitProperties;
import com.yizhaoqi.smartpai.exception.RateLimitExceededException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;
/**
 * 限流服务
 */

@Service
public class RateLimitService {

    private final StringRedisTemplate stringRedisTemplate;
    private final RateLimitProperties properties;
    private final RateLimitConfigService rateLimitConfigService;
    private final UsageQuotaService usageQuotaService;

    public RateLimitService(
            StringRedisTemplate stringRedisTemplate,
            RateLimitProperties properties,
            RateLimitConfigService rateLimitConfigService,
            UsageQuotaService usageQuotaService
    ) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.properties = properties;
        this.rateLimitConfigService = rateLimitConfigService;
        this.usageQuotaService = usageQuotaService;
    }

    public void checkRegisterByIp(String ip) {
        checkSingleWindow("register:ip:" + ip, properties.getRegister().getMax(), properties.getRegister().getWindowSeconds(), "注册请求过于频繁");
    }

    public void checkLoginByIp(String ip) {
        checkSingleWindow("login:ip:" + ip, properties.getLogin().getMax(), properties.getLogin().getWindowSeconds(), "登录请求过于频繁");
    }

    public void checkChatByUser(String userId) {
        RateLimitConfigService.WindowLimitView limit = rateLimitConfigService.getCurrentSettings().chatMessage();
        checkSingleWindow("chat:user:" + userId, limit.max(), limit.windowSeconds(), "聊天请求过于频繁");
        usageQuotaService.recordChatRequest(userId);
    }

    public UsageQuotaService.TokenReservationBundle reserveLlmUsage(
            String userId,
            int estimatedPromptTokens,
            int maxCompletionTokens
    ) {
        RateLimitConfigService.TokenBudgetView limit = rateLimitConfigService.getCurrentSettings().llmGlobalToken();
        return usageQuotaService.reserveLlmTokensWithGlobalBudget(
                userId,
                estimatedPromptTokens,
                maxCompletionTokens,
                limit.minuteMax(),
                limit.minuteWindowSeconds(),
                limit.dayMax(),
                limit.dayWindowSeconds()
        );
    }

    public void checkEmbeddingQueryByUser(String userId) {
        RateLimitConfigService.DualWindowLimitView limit = rateLimitConfigService.getCurrentSettings().embeddingQueryRequest();
        // 检查每分钟查询次数限制
        checkSingleWindow("embedding:query:min:user:" + userId, limit.minuteMax(), limit.minuteWindowSeconds(), "Embedding查询过于频繁");
        // 检查每天查询次数限制
        checkSingleWindow("embedding:query:day:user:" + userId, limit.dayMax(), limit.dayWindowSeconds(), "Embedding查询当日次数已达上限");
    }

    // 预留Embedding上传的Token用量，包含全网Token预算限制（不区分用户）
    public UsageQuotaService.TokenReservationBundle reserveEmbeddingUploadUsage(String userId, java.util.List<String> texts) {
        RateLimitConfigService.TokenBudgetView limit = rateLimitConfigService.getCurrentSettings().embeddingUploadToken();
        return usageQuotaService.reserveEmbeddingTokensWithGlobalBudget(
                userId,
                texts,
                "embedding-upload",
                "Embedding上传全网分钟Token预算已达上限",
                "Embedding上传全网当日Token预算已达上限",
                limit.minuteMax(),
                limit.minuteWindowSeconds(),
                limit.dayMax(),
                limit.dayWindowSeconds()
        );
    }

    /**
     * 预留Embedding查询的Token用量，包含单用户速率限制和全网Token预算限制
     * @param userId
     * @param texts
     * @return
     */
    public UsageQuotaService.TokenReservationBundle reserveEmbeddingQueryUsage(String userId, java.util.List<String> texts) {
       // 1. 检查单用户速率限制（每分钟和每天的查询次数）
        checkEmbeddingQueryByUser(userId);
        // 2. 预留Token用量（根据文本长度估算Token数），同时检查全网Token预算限制
        RateLimitConfigService.TokenBudgetView limit = rateLimitConfigService.getCurrentSettings().embeddingQueryGlobalToken();
        return usageQuotaService.reserveEmbeddingTokensWithGlobalBudget(
                userId,
                texts,
                "embedding-query",
                "Embedding查询全网分钟Token预算已达上限",
                "Embedding查询全网当日Token预算已达上限",
                limit.minuteMax(),
                limit.minuteWindowSeconds(),
                limit.dayMax(),
                limit.dayWindowSeconds()
        );
    }
    /**
     * 检查单窗口速率限制
     * @param key 速率限制键（redis key）: 业务类型 + ip/用户ID
     * @param max 最大请求数
     * @param windowSeconds 时间窗口（秒）
     * @param message 超出速率限制时的异常消息
     */
    private void checkSingleWindow(String key, long max, long windowSeconds, String message) {
        // 执行次数
        Long current = stringRedisTemplate.opsForValue().increment(key);

        // redis 挂了，直接返回
        if (current == null) {
            return;
        }

        if (current == 1) {
            stringRedisTemplate.expire(key, windowSeconds, TimeUnit.SECONDS);  // 第一次请求: 设置60秒过期
        }
        // 2. 如果超过限制就抛异常(ttl 是你在这个周期里被限流后还需要等的实际秒数, 一直在递减， windowsSeconds 是你在这个周期里被限流后需要等的最大秒数-固定)
        if (current > max) {
            // 获取key的剩余过期时间（还有多久重置限流）
            //异常情况：ttl == null || ttl < 0:返回 -2 → key 不存在; -1 → key存在永久有效
            // 如果拿不到剩余时间，或者剩余时间是负数（异常），
            // 就让用户等待 完整的时间窗口；
           //  否则，就让用户等待 真实的剩余时间。
            Long ttl = stringRedisTemplate.getExpire(key, TimeUnit.SECONDS);
            long retryAfterSeconds = ttl == null || ttl < 0 ? windowSeconds : ttl;
            throw new RateLimitExceededException(message, retryAfterSeconds);
        }
    }
}
