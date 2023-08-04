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

package ai.sapper.cdc.common.utils;

import ai.sapper.cdc.common.model.ERunnerState;
import ai.sapper.cdc.common.model.RunnerState;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class DirectoryCleaner implements Runnable {
    private final RunnerState state = new RunnerState();
    private final String basePath;
    private final boolean recursive;
    private final long retention;
    private final long runInterval;
    private final List<Pattern> ignorePatterns = new ArrayList<>();

    private File baseDir;

    public DirectoryCleaner(@NonNull String basePath,
                            boolean recursive,
                            long retention,
                            long runInterval) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(basePath));
        Preconditions.checkArgument(retention > 0);
        Preconditions.checkArgument(runInterval > 0);
        this.basePath = basePath;
        this.retention = retention;
        this.recursive = recursive;
        this.runInterval = runInterval;
    }

    public DirectoryCleaner(@NonNull String basePath) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(basePath));
        this.basePath = basePath;
        this.recursive = true;
        this.retention = 5L * 60 * 60 * 1000;
        this.runInterval = 60 * 1000;
    }

    public DirectoryCleaner ignore(@NonNull String regex) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(regex));
        Pattern p = Pattern.compile(regex);
        ignorePatterns.add(p);
        return this;
    }

    @Override
    public void run() {
        state.setState(ERunnerState.Running);
        baseDir = new File(basePath);
        while (state.isRunning()) {
            try {
                Thread.sleep(runInterval);
                if (baseDir.exists()) {
                    cleanUp();
                }
            } catch (Throwable t) {
                DefaultLogger.error("Directory Cleaner stopped with error", t);
                DefaultLogger.stacktrace(t);
                state.error(t);
            }
        }
        DefaultLogger.warn(String.format("Directory Cleaner stopped. [path=%s]", baseDir.getAbsolutePath()));
    }

    public void stop() {
        if (state.isRunning()) {
            state.setState(ERunnerState.Stopped);
        }
    }

    private void cleanUp() throws Exception {
        Collection<File> files = FileUtils.listFiles(baseDir, null, recursive);
        if (files != null && !files.isEmpty()) {
            for (File file : files) {
                if (file.isDirectory() && !recursive) continue;
                if (ignore(file)) continue;
                cleanUp(file);
            }
        }
    }

    private boolean ignore(File file) {
        for (Pattern p : ignorePatterns) {
            Matcher m = p.matcher(file.getAbsolutePath());
            if (m.matches()) return true;
        }
        return false;
    }

    private void cleanUp(File file) throws Exception {
        Path path = file.toPath();
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
        long mtime = attrs.lastModifiedTime().toMillis();
        long rtime = attrs.lastAccessTime().toMillis();
        long t = Math.max(mtime, rtime);
        if (System.currentTimeMillis() - t > retention) {
            if (!file.delete()) {
                DefaultLogger.warn(String.format("Failed to cleanup file. [path=%s]", file.getAbsolutePath()));
            } else {
                DefaultLogger.trace(String.format("Cleaned up file. [path=%s]", file.getAbsolutePath()));
            }
        }
    }
}
