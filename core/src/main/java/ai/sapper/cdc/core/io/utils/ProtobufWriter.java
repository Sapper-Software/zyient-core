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

package ai.sapper.cdc.core.io.utils;

import ai.sapper.cdc.core.io.Writer;
import com.google.protobuf.GeneratedMessageV3;
import lombok.NonNull;

import java.io.Closeable;
import java.io.IOException;

public class ProtobufWriter<T extends GeneratedMessageV3> implements Closeable {
    private final Writer writer;

    public ProtobufWriter(@NonNull Writer writer) throws Exception {
        this.writer = writer;
    }

    public void write(@NonNull T data) throws IOException {
        data.writeDelimitedTo(writer);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
