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

package io.zyient.base.core.connections.settings.hadoop;

import io.zyient.base.common.config.ConfigValueParser;
import io.zyient.base.common.utils.DefaultLogger;
import lombok.NonNull;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class HdfsUrlParser  implements ConfigValueParser<String[][]> {
    public String[][] parse(@NonNull String value) throws Exception {
        String[][] r = new String[2][2];
        String[] nns = value.split(";");
        if (nns.length != 2) {
            throw new ConfigurationException(
                    String.format("Invalid NameNode(s) specified. Expected count = 2, specified = %d",
                            nns.length));
        }
        for (int ii = 0; ii < nns.length; ii++) {
            String n = nns[ii];
            String[] parts = n.split("=");
            if (parts.length != 2) {
                throw new ConfigurationException(
                        String.format("Invalid NameNode specified. Expected count = 2, specified = %d",
                                parts.length));
            }
            String key = parts[0].trim();
            String address = parts[1].trim();

            DefaultLogger.info(String.format("Registering namenode [%s -> %s]...", key, address));
            r[ii][0] = key;
            r[ii][1] = address;
        }
        return r;
    }

    public String serialize(@NonNull String[][] source) throws Exception {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String[] nn : source) {
            if (first) first = false;
            else {
                builder.append(";");
            }
            if (nn.length != 2) {
                throw new Exception(String.format("Invalid URL definition: [url=%s]", (Object) nn));
            }
            builder.append(nn[0]).append("=").append(nn[1]);
        }
        return builder.toString();
    }
}
