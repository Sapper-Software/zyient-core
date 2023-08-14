/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.cdc.core.connections.settings.mail;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.mail.Session;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Properties;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SMTPConnectionSettings extends MailConnectionSettings {
    public static final String CONFIG_SOCKET_FACTORY_PARAM = "mail.smtp.socketFactory.class";
    public static final String CONFIG_SOCKET_FALLBACK_PARAM = "mail.smtp.socketFactory.fallback";
    public static final String CONFIG_SOCKET_PORT_PARAM = "mail.stmp.socketFactory.port";
    public static final String CONFIG_SMTP_PORT_PARAM = "mail.smtp.port";
    public static final String CONFIG_SMTP_HOST_PARAM = "mail.smtp.host";
    public static final String CONFIG_SMTPS_PORT_PARAM = "mail.smtps.port";
    public static final String CONFIG_SMTPS_HOST_PARAM = "mail.smtps.host";
    public static final String CONFIG_ENCRYPTION_TYPE = "mail.smtp.starttls.enable";
    public static final String CONFIG_SMTP_AUTH = "mail.smtp.auth";
    public static final String CONFIG_SMTPS_AUTH = "mail.smtps.auth";
    public static final String CONFIG_SMTP_PROTOCOL = "smtp";
    public static final String CONFIG_SMTPS_PROTOCOL = "smtps";
    public static final String CONFIG_SSL_TRUST = "mail.smtp.ssl.trust";

    public static final String __CONFIG_PATH = "smtp";

    public Properties get() {
        Properties props = new Properties();
        props.setProperty(CONFIG_SMTP_HOST_PARAM, mailServer);
        props.setProperty(CONFIG_SMTP_PORT_PARAM, String.valueOf(port));
        props.put(CONFIG_SMTP_AUTH, String.valueOf(true));
        if (useSSL) {
            props.setProperty(CONFIG_SOCKET_FACTORY_PARAM,
                    javax.net.ssl.SSLSocketFactory.class.getCanonicalName());
            props.setProperty(CONFIG_SOCKET_FALLBACK_PARAM, String.valueOf(false));
            props.setProperty(CONFIG_SOCKET_PORT_PARAM, String.valueOf(port));
        }
        if (useTLS) {
            props.setProperty(CONFIG_ENCRYPTION_TYPE, String.valueOf(true));
        }
        if (getParameters() != null && !getParameters().isEmpty()) {
            Map<String, String> params = getParameters();
            props.putAll(params);
        }
        return props;
    }
}
