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

package io.zyient.core.filesystem.impl.local;

import com.google.common.base.Preconditions;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.PathsBuilder;
import io.zyient.core.filesystem.model.Inode;
import io.zyient.core.filesystem.model.PathInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class LocalPathInfo extends PathInfo {
    protected File file;

    public LocalPathInfo(@NonNull FileSystem fs,
                         @NonNull Inode node) {
        super(fs, node);
        file = new File(path());
        if (!file.exists()) {
            directory = file.isDirectory();
        }
    }

    public LocalPathInfo(@NonNull FileSystem fs,
                         @NonNull String path,
                         @NonNull String domain) {
        super(fs, path, domain);
        PathsBuilder builder = fs.pathsBuilders().get(domain);
        String fsPath = builder.buildFsPath(path);
        file = new File(fsPath);
        if (file.exists()) {
            directory = file.isDirectory();
        }
    }

    public LocalPathInfo(@NonNull FileSystem fs,
                         @NonNull Map<String, String> config) {
        super(fs, config);
        PathsBuilder builder = fs.pathsBuilders().get(domain());
        String fsPath = builder.buildFsPath(path());
        file = new File(fsPath);
        if (file.exists()) {
            directory = file.isDirectory();
        }
    }

    protected LocalPathInfo(@NonNull FileSystem fs,
                            @NonNull File file,
                            @NonNull String domain) {
        super(fs, file.getAbsolutePath(), domain);
        PathsBuilder builder = fs.pathsBuilders().get(domain());
        String path = builder.relativeFsPath(file.getAbsolutePath());
        path(path);
        this.file = file;
        if (file.exists()) {
            directory = file.isDirectory();
        }
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public boolean exists() throws IOException {
        Preconditions.checkNotNull(fs());
        return fs().exists(this);
    }

    /**
     * @return
     * @throws IOException
     */
    @Override
    public long size() throws IOException {
        Path p = Paths.get(file.toURI());
        dataSize(Files.size(p));
        return dataSize();
    }

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return super.toString();
    }
}
