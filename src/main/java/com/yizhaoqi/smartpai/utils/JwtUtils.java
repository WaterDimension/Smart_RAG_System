package com.yizhaoqi.smartpai.utils;

import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.yizhaoqi.smartpai.model.User;
import com.yizhaoqi.smartpai.repository.UserRepository;
import com.yizhaoqi.smartpai.service.TokenCacheService;

import javax.crypto.SecretKey;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
/**
 *  负责生成、验证和解析 JWT 令牌
 */
@Component
public class JwtUtils {
    private static final Logger logger = LoggerFactory.getLogger(JwtUtils.class);

    @Value("${jwt.secret-key}")
    private String secretKeyBase64; // 这里存的是 Base64 编码后的密钥

    private static final long EXPIRATION_TIME = 3600000; // 1 hour (调整为1小时)
    private static final long REFRESH_TOKEN_EXPIRATION_TIME = 604800000; // 7 days (refresh token有效期)
    private static final long REFRESH_THRESHOLD = 300000; // 5分钟：当剩余时间少于5分钟时开始刷新
    private static final long REFRESH_WINDOW = 600000; // 10分钟：token过期后的宽限期
    
    @Autowired
    private UserRepository userRepository;
    
    @Autowired
    private TokenCacheService tokenCacheService;

    /**
     * 解析 Base64 密钥，并返回 SecretKey
     * 还原 + 格式转换: 先解析 Base64 → 再用 hmacShaKeyFor转换为 SecretKey 对象
     */
    private SecretKey getSigningKey() {
        // 1. 将一串经过 Base64 编码的长字符串，还原成原始的二进制字节数组（byte[]）
        byte[] keyBytes = Base64.getDecoder().decode(secretKeyBase64);
        // 2. JJWT 验证这串字节是否足够长，安全的话就将其包装成符合 HMAC-SHA 算法要求的官方 SecretKey 对象
        return Keys.hmacShaKeyFor(keyBytes);
    }

    /**
     * 生成 JWT Token（集成Redis缓存）
     */
    public String generateToken(String username) {
        //
        SecretKey key = getSigningKey(); // 获取密钥
        
        // 获取用户信息
        User user = userRepository.findByUsername(username)
                .orElseThrow(() -> new RuntimeException("User not found"));
        
        // 生成唯一的tokenId
        String tokenId = generateTokenId();
        long expireTime = System.currentTimeMillis() + EXPIRATION_TIME; // 1小时过期
        
        // 创建token的 Payload  （ Header + Payload + Signature）
        Map<String, Object> claims = new HashMap<>();
        claims.put("tokenId", tokenId); // 添加tokenId用于Redis缓存
        claims.put("role", user.getRole().name());
        claims.put("userId", user.getId().toString()); // 添加用户ID到JWT
        
        // 添加组织标签信息
        if (user.getOrgTags() != null && !user.getOrgTags().isEmpty()) {
            claims.put("orgTags", user.getOrgTags());
        }
        
        // 添加主组织标签信息
        if (user.getPrimaryOrg() != null && !user.getPrimaryOrg().isEmpty()) {
            claims.put("primaryOrg", user.getPrimaryOrg());
        }

        //封装权限信息，
        // 这种设计使得 JWT 令牌成为一个自包含的权限载体，避免频繁的数据库查询。
        String token = Jwts.builder()
                .setClaims(claims)
                .setSubject(username)     //name设置成设置为 JWT 的 subject
                .setExpiration(new Date(expireTime))
                .signWith(key, SignatureAlgorithm.HS256)
                .compact();
        
        // JWT 自身无法实现主动失效，需要在Redis缓存中维护token状态，用户登陆需要双重验证token状态
        // redis用来判断主动失效的token(哪怕 Token 没过期，接口也直接拦截)
        // jwt做兜底，避免tokenId过期后还能正常登陆（redis只查主动失效的黑名单）
        // 缓存token信息到Redis
        tokenCacheService.cacheToken(tokenId, user.getId().toString(), username, expireTime);
        
        logger.info("Token generated and cached for user: {}, tokenId: {}", username, tokenId);
        return token;
    }

    /**
     * 验证 JWT Token 是否有效（优先使用Redis缓存）
     * 需要查询 Redis 确认是否主动失效，再验证JWT签名（双重验证）确认是否过期
     */
    public boolean validateToken(String token) {
        try {
            // 首先从JWT中提取tokenId，不论是否过期，都返回（快速失败）
            // jwt天生有缺陷，必须靠 tokenId+Redis 解决，而拿 tokenId 的唯一方式，就是忽略过期提取它。
            // 要查 Redis 黑名单，必须先拿到 tokenId
            String tokenId = extractTokenIdFromToken(token);
            if (tokenId == null) {
                logger.warn("Token does not contain tokenId");
                return false;
            }
            
            // 检查Redis缓存中的token状态，是否主动失效(在黑名单中记录)
            if (!tokenCacheService.isTokenValid(tokenId)) {
                logger.debug("Token invalid in cache: {}", tokenId);
                return false;
            }
            
            // Redis验证通过，再验证JWT签名（双重验证）
            // 这里自动失效的token能过redis(黑名单中没有)，但会被ExpiredJwtException真正拦截
            Jwts.parserBuilder()
                    .setSigningKey(getSigningKey())
                    .build()
                    .parseClaimsJws(token);

            logger.debug("Token validation successful: {}", tokenId);
            return true;
        } catch (ExpiredJwtException e) {
            logger.warn("Token expired: {}", e.getClaims().get("tokenId", String.class));
        } catch (SignatureException e) {
            logger.warn("Invalid token signature");
        } catch (Exception e) {
            logger.error("Error validating token", e);
        }
        return false;
    }

    /**
     * 从 JWT Token 中提取用户名
     */
    public String extractUsernameFromToken(String token) {
        try {
            Claims claims = extractClaimsIgnoreExpiration(token);  //payload
            return claims != null ? claims.getSubject() : null;
        } catch (Exception e) {
            logger.error("Error extracting username from token: {}", token, e);
            return null;
        }
    }
    
    /**
     * 从 JWT Token 中提取用户ID
     */
    public String extractUserIdFromToken(String token) {
        try {
            Claims claims = extractClaimsIgnoreExpiration(token);
            return claims != null ? claims.get("userId", String.class) : null;
        } catch (Exception e) {
            logger.error("Error extracting userId from token: {}", token, e);
            return null;
        }
    }
    
    /**
     * 从 JWT Token 中提取用户角色
     */
    public String extractRoleFromToken(String token) {
        try {
            Claims claims = extractClaimsIgnoreExpiration(token);
            return claims != null ? claims.get("role", String.class) : null;
        } catch (Exception e) {
            logger.error("Error extracting role from token: {}", token, e);
            return null;
        }
    }
    
    /**
     * 从 JWT Token 中提取组织标签
     */
    public String extractOrgTagsFromToken(String token) {
        try {
            Claims claims = extractClaimsIgnoreExpiration(token);
            return claims != null ? claims.get("orgTags", String.class) : null;
        } catch (Exception e) {
            logger.error("Error extracting organization tags from token: {}", token, e);
            return null;
        }
    }
    
    /**
     * 从 JWT Token 中提取主组织标签
     */
    public String extractPrimaryOrgFromToken(String token) {
        try {
            Claims claims = extractClaimsIgnoreExpiration(token);
            return claims != null ? claims.get("primaryOrg", String.class) : null;
        } catch (Exception e) {
            logger.error("Error extracting primary organization from token: {}", token, e);
            return null;
        }
    }
    
    /**
     * 检查未过期token是否应该刷新（剩余时间少于阈值）
     */
    public boolean shouldRefreshToken(String token) {
        try {
            Claims claims = extractClaims(token);
            if (claims == null) return false;
            
            long expirationTime = claims.getExpiration().getTime();
            long currentTime = System.currentTimeMillis();
            // 计算剩余有效时间
            long remainingTime = expirationTime - currentTime;
            // 未过期 + 剩余时间小于刷新阈值 → 需要刷新
            return remainingTime > 0 && remainingTime < REFRESH_THRESHOLD; // 5分钟
        } catch (Exception e) {
            logger.debug("Cannot check if token should refresh: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * 检查过期的token是否仍可刷新（在宽限期内）
     */
    public boolean canRefreshExpiredToken(String token) {
        try {
            Claims claims = extractClaimsIgnoreExpiration(token);
            if (claims == null) return false;
            
            long expirationTime = claims.getExpiration().getTime();
            long currentTime = System.currentTimeMillis();
            long expiredTime = currentTime - expirationTime;
            // 过期时间小于刷新宽限期 → 可刷新
            //过期10 分钟内可刷新
            return expiredTime > 0 && expiredTime < REFRESH_WINDOW;
        } catch (Exception e) {
            logger.debug("Cannot check if expired token can refresh: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * 刷新token（生成新的token）
     */
    public String refreshToken(String oldToken) {
        try {
            Claims claims = extractClaimsIgnoreExpiration(oldToken);
            if (claims == null) return null;
            
            String username = claims.getSubject();
            if (username == null || username.isEmpty()) return null;
            
            // 重新生成token
            String newToken = generateToken(username);
            logger.info("Token refreshed successfully for user: {}", username);
            return newToken;
        } catch (Exception e) {
            logger.error("Error refreshing token: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * 提取Claims，忽略过期异常
     */
    private Claims extractClaimsIgnoreExpiration(String token) {
        try {
            return Jwts.parserBuilder()
                    .setSigningKey(getSigningKey())
                    .build()
                    .parseClaimsJws(token)
                    .getBody();
        } catch (ExpiredJwtException e) {
            // 忽略过期异常，返回claims
            return e.getClaims();
        } catch (Exception e) {
            logger.debug("Cannot extract claims from token: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * 提取Claims（正常验证）
     */
    private Claims extractClaims(String token) {
        try {
            return Jwts.parserBuilder()
                    .setSigningKey(getSigningKey())
                    .build()
                    .parseClaimsJws(token)
                    .getBody();
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * 生成 Refresh Token（长期有效的刷新令牌，集成Redis缓存）
     */
    public String generateRefreshToken(String username) {
        SecretKey key = getSigningKey();
        
        // 获取用户信息
        User user = userRepository.findByUsername(username)
                .orElseThrow(() -> new RuntimeException("User not found"));
        
        // 生成唯一的refreshTokenId
        String refreshTokenId = generateTokenId();
        long expireTime = System.currentTimeMillis() + REFRESH_TOKEN_EXPIRATION_TIME; // 7天过期
        
        // 创建refreshToken内容（相对简单，只包含基本信息）
        Map<String, Object> claims = new HashMap<>();
        claims.put("refreshTokenId", refreshTokenId); // 添加refreshTokenId
        claims.put("userId", user.getId().toString());
        claims.put("type", "refresh"); // 标识这是一个refresh token

        String refreshToken = Jwts.builder()
                .setClaims(claims)
                .setSubject(username)
                .setExpiration(new Date(expireTime))
                .signWith(key, SignatureAlgorithm.HS256)
                .compact();
        
        // 缓存refresh token信息到Redis
        tokenCacheService.cacheRefreshToken(refreshTokenId, user.getId().toString(), null, expireTime);
        
        logger.info("Refresh token generated and cached for user: {}, refreshTokenId: {}", username, refreshTokenId);
        return refreshToken;
    }
    
    /**
     * 验证 Refresh Token 是否有效（优先使用Redis缓存）
     */
    public boolean validateRefreshToken(String refreshToken) {
        try {
            // 首先从JWT中提取refreshTokenId
            String refreshTokenId = extractRefreshTokenIdFromToken(refreshToken);
            if (refreshTokenId == null) {
                logger.warn("Refresh token does not contain refreshTokenId");
                return false;
            }
            
            // 检查Redis缓存中的refresh token状态
            if (!tokenCacheService.isRefreshTokenValid(refreshTokenId)) {
                logger.debug("Refresh token invalid in cache: {}", refreshTokenId);
                return false;
            }
            
            // Redis验证通过，再验证JWT签名
            Claims claims = Jwts.parserBuilder()
                    .setSigningKey(getSigningKey())
                    .build()
                    .parseClaimsJws(refreshToken)
                    .getBody();
            
            // 验证是否为refresh token类型
            String tokenType = claims.get("type", String.class);
            if (!"refresh".equals(tokenType)) {
                logger.warn("Token is not a refresh token");
                return false;
            }

            logger.debug("Refresh token validation successful: {}", refreshTokenId);
            return true;
        } catch (ExpiredJwtException e) {
            logger.warn("Refresh token expired: {}", e.getClaims().get("refreshTokenId", String.class));
        } catch (SignatureException e) {
            logger.warn("Invalid refresh token signature");
        } catch (Exception e) {
            logger.error("Error validating refresh token", e);
        }
        return false;
    }
    
    /**
     * 从 JWT Token 中提取refreshTokenId
     */
    public String extractRefreshTokenIdFromToken(String refreshToken) {
        try {
            Claims claims = extractClaimsIgnoreExpiration(refreshToken);
            return claims != null ? claims.get("refreshTokenId", String.class) : null;
        } catch (Exception e) {
            logger.debug("Error extracting refreshTokenId from token", e);
            return null;
        }
    }
    
    /**
     * 生成唯一的tokenId
     */
    private String generateTokenId() {
        return UUID.randomUUID().toString().replace("-", "");
    }
    
    /**
     * 从 JWT Token 中提取tokenId
     */
    public String extractTokenIdFromToken(String token) {
        try {
            Claims claims = extractClaimsIgnoreExpiration(token);
            return claims != null ? claims.get("tokenId", String.class) : null;
        } catch (Exception e) {
            logger.debug("Error extracting tokenId from token", e);
            return null;
        }
    }
    
    /**
     * 使token失效（加入Redis黑名单）
     */
    public void invalidateToken(String token) {
        try {
            String tokenId = extractTokenIdFromToken(token);
            if (tokenId != null) {
                Claims claims = extractClaimsIgnoreExpiration(token);
                if (claims != null) {
                    long expireTime = claims.getExpiration().getTime();
                    String userId = claims.get("userId", String.class);
                    
                    // 加入黑名单
                    tokenCacheService.blacklistToken(tokenId, expireTime);
                    // 从缓存中移除
                    tokenCacheService.removeToken(tokenId, userId);
                    
                    logger.info("Token invalidated: {}", tokenId);
                }
            }
        } catch (Exception e) {
            logger.error("Error invalidating token", e);
        }
    }
    
    /**
     * 使用户所有token失效（批量登出）
     */
    public void invalidateAllUserTokens(String userId) {
        try {
            tokenCacheService.removeAllUserTokens(userId);
            logger.info("All tokens invalidated for user: {}", userId);
        } catch (Exception e) {
            logger.error("Error invalidating all user tokens: {}", userId, e);
        }
    }
}
