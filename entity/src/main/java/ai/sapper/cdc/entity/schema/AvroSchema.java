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

package ai.sapper.cdc.entity.schema;

import ai.sapper.cdc.common.utils.ChecksumUtils;
import ai.sapper.cdc.common.utils.DefaultLogger;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.Schema;
import org.slf4j.event.Level;

import java.util.List;

@Getter
@Setter
public class AvroSchema {
    private SchemaVersion version;
    private String hash;
    private String schemaStr;
    private String zkPath;
    @JsonIgnore
    private Schema schema;

    public AvroSchema withSchema(@NonNull Schema schema) throws Exception {
        schemaStr = schema.toString(false);
        hash = ChecksumUtils.generateHash(schemaStr.replaceAll("\\s", ""));
        this.schema = schema;
        return this;
    }

    public AvroSchema withSchemaStr(@NonNull String schemaStr) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(schemaStr));
        this.schemaStr = schemaStr;
        load();
        hash = ChecksumUtils.generateHash(schemaStr);
        return this;
    }

    public AvroSchema load() throws Exception {
        Preconditions.checkState(!Strings.isNullOrEmpty(schemaStr));
        schema = new Schema.Parser().parse(schemaStr);
        return this;
    }

    public boolean compare(@NonNull AvroSchema target)
            throws Exception {
        Preconditions.checkNotNull(this.schema);
        Preconditions.checkNotNull(target.schema);
        if (hash.compareTo(target.hash) != 0) {
            return false;
        }
        List<SchemaEvolutionValidator.Message> response =
                SchemaEvolutionValidator
                        .checkBackwardCompatibility(this.schema,
                                target.schema,
                                this.schema.getName());
        if (response.isEmpty()) return true;
        Level maxLevel = Level.DEBUG;
        for (SchemaEvolutionValidator.Message message : response) {
            if (DefaultLogger.isGreaterOrEqual(message.getLevel(), maxLevel)) {
                maxLevel = message.getLevel();
            }
        }
        return DefaultLogger.isGreaterOrEqual(Level.DEBUG, maxLevel);
    }
}
