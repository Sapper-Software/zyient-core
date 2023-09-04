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

package io.zyient.intake.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class EmailConnectionJson {
    @Nonnull
    private String serverId;
    @Nonnull
    private String name;
    @Nonnull
    private String emailId;
    @Nonnull
    private String channel;
    private String domain;
    @Nonnull
    private String account;
    @Nonnull
    private short partition;
    @Nonnull
    private String password;
    private String proxyUser;
    private String delegateUser;
    @Nonnull
    private String emailType;
    private String tenantId;

    @Override
    public String toString() {
        return String.format("serverId: %s," +
                "name: %s," +
                "emailId: %s," +
                "channel: %s," +
                "domain: %s," +
                "account: %s," +
                "proxyUser: %s," +
                "delegateUser: %s," +
                "emailType : %s," +
                "tenantId : %s", serverId,name,emailId,channel,domain,account,proxyUser,delegateUser,emailType,tenantId);
    }
}
