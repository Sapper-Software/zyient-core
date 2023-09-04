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

package io.zyient.base.core.io.model;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.io.FileSystem;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class PathInfo {
    public static final String CONFIG_KEY_TYPE = "type";
    public static final String CONFIG_KEY_DOMAIN = "domain";
    public static final String CONFIG_KEY_PATH = "path";
    public static final String CONFIG_IS_DIRECTORY = "isDirectory";
    public static final String CONFIG_KEY_UUID = "UUID";

    private final FileSystem fs;
    private final String domain;
    private final String path;
    private final String uuid;
    protected boolean directory = false;
    private long dataSize = -1;

    protected PathInfo(@NonNull FileSystem fs,
                       @NonNull Inode node) {
        this.fs = fs;
        this.directory = node.isDirectory();
        domain = node.getDomain();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(domain));
        path = node.getAbsolutePath();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        uuid = node.getUuid();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(uuid));
    }

    protected PathInfo(@NonNull FileSystem fs,
                       @NonNull String path,
                       @NonNull String domain) {
        this.fs = fs;
        this.path = PathUtils.formatPath(path);
        this.domain = domain;
        this.uuid = UUID.randomUUID().toString();
    }

    protected PathInfo(@NonNull FileSystem fs,
                       @NonNull Map<String, String> config) {
        this.fs = fs;
        domain = config.get(CONFIG_KEY_DOMAIN);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(domain));
        path = config.get(CONFIG_KEY_PATH);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        directory = Boolean.parseBoolean(config.get(CONFIG_IS_DIRECTORY));
        uuid = config.get(CONFIG_KEY_UUID);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(uuid));
    }

    public String parent() {
        return FilenameUtils.getFullPath(path);
    }


    public String name() {
        return FilenameUtils.getName(path);
    }

    public String extension() {
        return FilenameUtils.getExtension(path);
    }

    public abstract boolean exists() throws IOException;

    public abstract long size() throws IOException;

    public Map<String, String> pathConfig() {
        Map<String, String> config = new HashMap<>();
        config.put(CONFIG_KEY_TYPE, getClass().getCanonicalName());
        config.put(CONFIG_KEY_DOMAIN, domain);
        config.put(CONFIG_KEY_PATH, path);
        config.put(CONFIG_KEY_UUID, uuid);
        config.put(CONFIG_IS_DIRECTORY, String.valueOf(directory));

        return config;
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
        return "{domain=" + domain + ", path=" + path + "}";
    }
}
