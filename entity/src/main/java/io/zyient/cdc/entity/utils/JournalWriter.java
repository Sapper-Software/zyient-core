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

package io.zyient.cdc.entity.utils;

import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.io.FileSystem;
import io.zyient.base.core.io.Writer;
import io.zyient.base.core.io.model.DirectoryInode;
import io.zyient.base.core.io.model.FileInode;
import io.zyient.cdc.entity.schema.SchemaEntity;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class JournalWriter<T> implements Closeable {
    public static class Constants {
        public static final String __PATH_FORMAT_HOUR = "yyyy/MM/dd/HH";
        public static final String __PATH_FORMAT_DAY = "yyyy/MM/dd";
        public static final String __PATH_FORMAT_MONTH = "yyyy/MM";
    }

    public enum EPathFormat {
        Month, Day, Hour, Minutes30, Minutes15;

        public static String path(@NonNull EPathFormat format) {
            return switch (format) {
                case Day -> DateTimeHelper.dateString(Constants.__PATH_FORMAT_DAY);
                case Month -> DateTimeHelper.dateString(Constants.__PATH_FORMAT_MONTH);
                case Hour -> DateTimeHelper.dateString(Constants.__PATH_FORMAT_HOUR);
                case Minutes30 -> getMinuteOffset(30);
                case Minutes15 -> getMinuteOffset(15);
            };
        }

        private static String getMinuteOffset(int b) {
            String dt = DateTimeHelper.dateString(Constants.__PATH_FORMAT_HOUR);
            DateTime d = new DateTime(System.currentTimeMillis());
            int m = d.minuteOfHour().get();
            int o = m / b;
            m *= o;
            return String.format("%s/%d", dt, m);
        }
    }

    private final BaseEnv<?> env;
    private final FileSystem fs;
    private final SchemaEntity entity;
    private final EPathFormat format;
    private final DirectoryInode root;
    private FileInode node;
    private Writer writer;

    protected JournalWriter(@NonNull BaseEnv<?> env,
                            @NonNull FileSystem fs,
                            @NonNull DirectoryInode root,
                            @NonNull SchemaEntity entity,
                            @NonNull EPathFormat format) {
        this.env = env;
        this.fs = fs;
        this.root = root;
        this.entity = entity;
        this.format = format;
    }

    public JournalWriter<T> open() throws IOException {
        String path = EPathFormat.path(format);
        node = fs.create(root, path);
        writer = fs.writer(node);
        return this;
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.commit(true);
            writer.close();
            writer = null;
        }
    }

    public abstract void write(@NonNull T record) throws IOException;

    public abstract void write(@NonNull List<T> records) throws IOException;
}
