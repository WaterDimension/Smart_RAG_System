package com.yizhaoqi.smartpai.config;

import com.yizhaoqi.smartpai.model.RegistrationMode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
/**
 * 应用认证属性配置类
 * 对应 application.properties 中的 app.auth 配置项
 */
@Component
@ConfigurationProperties(prefix = "app.auth")
public class AppAuthProperties {

    private final Registration registration = new Registration();
    /**
     * 获取注册属性
     * @return 注册属性
     */
    public Registration getRegistration() {
        return registration;
    }
    /**
     * 注册属性类
     * 对应 application.yml 中的 app.auth.registration 配置项
     * 如果application.yml写，默认是 INVITE_ONLY 模式
     */
    public static class Registration {
        private RegistrationMode mode = RegistrationMode.INVITE_ONLY;
        private boolean inviteRequired = true;
        /**
         * 注册模式
         * @return 注册模式
         */
        public RegistrationMode getMode() {
            return mode;
        }

        public void setMode(RegistrationMode mode) {
            this.mode = mode;
        }

        public boolean isInviteRequired() {
            return inviteRequired;
        }

        public void setInviteRequired(boolean inviteRequired) {
            this.inviteRequired = inviteRequired;
        }
    }
}
