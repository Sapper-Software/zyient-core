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

package io.zyient.core.persistence.impl.settings.mail;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.config.Config;
import io.zyient.base.core.connections.settings.EConnectionType;
import io.zyient.core.persistence.AbstractConnectionSettings;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class MailConnectionSettings extends AbstractConnectionSettings {
    @Config(name = "server")
    protected String mailServer;
    @Config(name = "port", type = Integer.class)
    protected int port = -1;
    @Config(name = "useSSL", required = false, type = Boolean.class)
    protected boolean useSSL = false;
    @Config(name = "useTLS", required = false, type = Boolean.class)
    protected boolean useTLS = false;
    // john.smith@contoso.com\info@contoso.com (user\shared mailbox)
    @Config(name = "username")
    protected String username;
    @Config(name = "passkey")
    protected String passkey;
    @Config(name = "emailId")
    protected String emailId;
    @Config(name = "requestTimeout")
    protected int requestTimeout;
    @Config(name = "useCredentials", required = false, type = Boolean.class)
    protected boolean useCredentials = true;

    protected MailConnectionSettings() {
        setType(EConnectionType.email);
    }
}
