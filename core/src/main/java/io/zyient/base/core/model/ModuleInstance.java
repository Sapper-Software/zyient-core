/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

@Getter
@Setter
public class ModuleInstance {
    private String module;
    private String name;
    private String ip;
    private String startTime;
    private String instanceId;

    public ModuleInstance() {
        instanceId = UUID.randomUUID().toString();
    }

    public ModuleInstance withStartTime(long startTime) {
        Date date = new Date(startTime);
        DateFormat df = new SimpleDateFormat("yyyyMMdd:HH:mm");
        this.startTime = df.format(date);
        return this;
    }

    public ModuleInstance withIp(InetAddress address) {
        if (address != null) {
            ip = address.toString();
        }
        return this;
    }

    public String id() {
        return String.format("%s/%s/%s", module, name, ip);
    }

    @JsonIgnore
    public boolean isInstance(@NonNull ModuleInstance target) {
        return (instanceId.compareTo(target.instanceId) == 0);
    }

    @JsonIgnore
    public boolean isModule(@NonNull ModuleInstance target) {
        boolean ret = false;
        if (module.compareTo(target.module) == 0) {
            if (name.compareTo(target.name) == 0) {
                ret = true;
            }
        }
        return ret;
    }

    @Override
    public String toString() {
        return String.format("[module=%s][name=%s], ID=%s", module, name, instanceId);
    }
}
