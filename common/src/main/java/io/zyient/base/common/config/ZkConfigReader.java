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

package io.zyient.base.common.config;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.JSONUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.curator.framework.CuratorFramework;

@Getter
@Accessors(fluent = true)
public final class ZkConfigReader {
    private final CuratorFramework client;
    private final Class<? extends Settings> type;
    private Settings settings;

    public ZkConfigReader(@NonNull CuratorFramework client,
                          @NonNull Class<? extends Settings> type) {
        this.client = client;
        this.type = type;
    }

    public void save(@NonNull String path) throws ConfigurationException {
        Preconditions.checkNotNull(settings);
        try {
            if (client.checkExists().forPath(path) != null) {
                client.create().creatingParentContainersIfNeeded().forPath(path);
            }
            byte[] data = JSONUtils.asBytes(settings);
            client.setData().forPath(path, data);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public boolean read(@NonNull String path) throws ConfigurationException {
        try {
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    settings = JSONUtils.read(data, type);
                    return true;
                }
            }
            return false;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
