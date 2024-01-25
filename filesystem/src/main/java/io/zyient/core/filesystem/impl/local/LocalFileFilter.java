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

package io.zyient.core.filesystem.impl.local;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
@Accessors(fluent = true)
public class LocalFileFilter implements IOFileFilter {
    private final String regex;
    private String dirRegex;
    private final Pattern pattern;
    @Setter(AccessLevel.NONE)
    private Pattern dirPatter;

    public LocalFileFilter(@NonNull String regex) {
        this.regex = regex;
        this.pattern = Pattern.compile(regex);
    }

    /**
     * Checks to see if the File should be accepted by this filter.
     * <p>
     * Defined in {@link FileFilter}.
     * </p>
     *
     * @param file the File to check.
     * @return true if this file matches the test.
     */
    @Override
    public boolean accept(File file) {
        if (file.isFile()) {
            if (matchDir(file.getParentFile().getAbsolutePath())) {
                String name = FilenameUtils.getName(file.getAbsolutePath());
                Matcher m = pattern.matcher(name);
                return m.matches();
            }
        }
        return false;
    }

    /**
     * Checks to see if the File should be accepted by this filter.
     * <p>
     * Defined in {@link FilenameFilter}.
     * </p>
     *
     * @param dir  the directory File to check.
     * @param name the file name within the directory to check.
     * @return true if this file matches the test.
     */
    @Override
    public boolean accept(File dir, String name) {
        if (matchDir(dir.getAbsolutePath())) {
            Matcher m = pattern.matcher(name);
            return m.matches();
        }
        return false;
    }

    private boolean matchDir(String path) {
        if (Strings.isNullOrEmpty(dirRegex)) return true;
        if (dirPatter == null) {
            dirPatter = Pattern.compile(dirRegex);
        }
        Matcher m = dirPatter.matcher(path);
        return m.matches();
    }
}
