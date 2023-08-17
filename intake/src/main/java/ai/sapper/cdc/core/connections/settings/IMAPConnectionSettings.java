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

package ai.sapper.cdc.core.connections.settings;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import java.util.Properties;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class IMAPConnectionSettings extends MailConnectionSettings {
    public static final String __CONFIG_PATH = "imap";

    public static final String CONFIG_MAIL_PROTOCOL_PARAM = "mail.store.protocol";
    public static final String CONFIG_SOCKET_FACTORY_PARAM = "mail.imap.socketFactory.class";
    public static final String CONFIG_SOCKET_FALLBACK_PARAM = "mail.imap.socketFactory.fallback";
    public static final String CONFIG_SOCKET_PORT_PARAM = "mail.imap.socketFactory.port";
    public static final String CONFIG_IMAP_PORT_PARAM = "mail.imap.port";
    public static final String CONFIG_IMAP_HOST_PARAM = "mail.imap.host";
    public static final String CONFIG_IMAP_USER_PARAM = "mail.imap.user";
    public static final String CONFIG_IMAPS_PORT_PARAM = "mail.imaps.port";
    public static final String CONFIG_IMAPS_HOST_PARAM = "mail.imaps.host";
    public static final String CONFIG_IMAPS_USER_PARAM = "mail.imaps.user";
    public static final String CONFIG_IMAP_SSL_PARAM = "mail.imap.ssl.enable";
    public static final String CONFIG_ENCRYPTION_TYPE = "mail.imap.starttls.enable";
    public static final String CONFIG_MAIL_PROTOCOL_SSL = "imaps";
    public static final String CONFIG_MAIL_PROTOCOL_NOSSL = "imap";

    public Properties get() {
        Properties props = new Properties();
        if (useSSL) {
            props.setProperty(CONFIG_IMAPS_HOST_PARAM, mailServer);
            props.setProperty(CONFIG_IMAPS_PORT_PARAM, String.valueOf(port));
            props.setProperty(CONFIG_MAIL_PROTOCOL_PARAM, CONFIG_MAIL_PROTOCOL_SSL);

            props.setProperty(CONFIG_IMAP_SSL_PARAM, String.valueOf(true));
            props.setProperty(CONFIG_SOCKET_FACTORY_PARAM,
                    javax.net.ssl.SSLSocketFactory.class.getCanonicalName());
            props.setProperty(CONFIG_SOCKET_FALLBACK_PARAM, String.valueOf(false));
            props.setProperty(CONFIG_SOCKET_PORT_PARAM, String.valueOf(port));
            props.setProperty(CONFIG_IMAPS_USER_PARAM, username);
        } else {
            props.setProperty(CONFIG_MAIL_PROTOCOL_PARAM, CONFIG_MAIL_PROTOCOL_NOSSL);
            props.setProperty(CONFIG_IMAP_HOST_PARAM, mailServer);
            props.setProperty(CONFIG_IMAP_PORT_PARAM, String.valueOf(port));
            props.setProperty(CONFIG_IMAP_USER_PARAM, username);
        }
        if (getParameters() != null && !getParameters().isEmpty()) {
            props.putAll(getParameters());
        }
        return props;
    }
}
