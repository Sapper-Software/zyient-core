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

package io.zyient.core.filesystem.impl.sftp;

import com.google.common.base.Preconditions;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.model.Inode;
import io.zyient.core.filesystem.model.InodeType;
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
public class SftpPathInfo extends PathInfo {
    private String host;
    private String username;
    private File temp;

    protected SftpPathInfo(@NonNull FileSystem fs,
                           @NonNull Inode node) {
        super(fs, node);
        init(fs);
    }

    protected SftpPathInfo(@NonNull FileSystem fs,
                           @NonNull String path,
                           @NonNull String domain) {
        super(fs, path, domain);
        init(fs);
    }

    protected SftpPathInfo(@NonNull FileSystem fs,
                           @NonNull String path,
                           @NonNull String domain,
                           @NonNull InodeType type) {
        super(fs, path, domain);
        directory = (type == InodeType.Directory);
        init(fs);
    }

    protected SftpPathInfo(@NonNull FileSystem fs,
                           @NonNull Map<String, String> config) {
        super(fs, config);
        init(fs);
    }

    private void  init(FileSystem fs) {
        SftpFileSystemSettings settings = (SftpFileSystemSettings) fs.settings();
        host = settings.getHost();
        username = settings.getUsername();
    }

    protected SftpPathInfo withTemp(@NonNull File temp) {
        this.temp = temp;
        return this;
    }

    @Override
    public boolean exists() throws IOException {
        Preconditions.checkNotNull(fs());
        return fs().exists(this);
    }

    @Override
    public long size() throws IOException {
        Preconditions.checkNotNull(fs());
        if (temp.exists()) {
            Path p = Paths.get(temp.toURI());
            dataSize(Files.size(p));
        } else {
            SftpFileSystem sfs = (SftpFileSystem) fs();
            dataSize(sfs.size(this));
        }
        return dataSize();
    }

    @Override
    public String uri() throws IOException {
        return String.format("sftp://%s@%s/%s", username, host, fsPath());
    }
}
