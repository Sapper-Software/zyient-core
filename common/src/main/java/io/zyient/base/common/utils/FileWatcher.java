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

package io.zyient.base.common.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class FileWatcher implements Runnable {
    private static final long __POLL_TIMEOUT = 100;

    private final String directory;
    private final String regex;
    private final Pattern pattern;

    private FileWatcherCallback callback;

    private boolean running = false;

    public FileWatcher(@NonNull String directory,
                       String regex) {
        File di = new File(directory);
        Preconditions.checkState(di.exists());

        this.directory = directory;
        this.regex = regex;
        if (!Strings.isNullOrEmpty(regex))
            this.pattern = Pattern.compile(regex);
        else
            pattern = null;
    }

    public FileWatcher withCallback(@NonNull FileWatcherCallback callback) {
        this.callback = callback;
        return this;
    }

    public void run() {
        Preconditions.checkState(callback != null);
        running = true;
        final Path path = Paths.get(directory);
        DefaultLogger.info(String.format("Starting file watcher. [directory=%s][regex=%s]", directory, regex));
        try (final WatchService watchService = FileSystems.getDefault().newWatchService()) {
            final WatchKey watchKey = path.register(watchService,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE,
                    StandardWatchEventKinds.ENTRY_CREATE);

            while (running) {
                final WatchKey wk = watchService.poll(__POLL_TIMEOUT, TimeUnit.MILLISECONDS);
                if (wk != null) {
                    for (WatchEvent<?> event : wk.pollEvents()) {
                        if (event.context() instanceof Path changed) {
                            final String name = changed.toFile().getName();
                            boolean call = false;
                            if (pattern != null) {
                                final Matcher matcher = pattern.matcher(name);
                                if (matcher.matches()) {
                                    call = true;
                                }
                            } else {
                                call = true;
                            }
                            if (call) {
                                Path cp = Paths.get(String.format("%s/%s", directory, changed.toString()));
                                callback.handle(name, cp.toAbsolutePath().toString(), event.kind());
                            }
                        }
                    }
                    // reset the key
                    boolean valid = wk.reset();
                    if (!valid) {
                        throw new IOException(String.format("[%s] Key no longer valid.", path.toAbsolutePath().toString()));
                    }
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        running = false;
    }

    public static interface FileWatcherCallback {
        void handle(@NonNull String watcher,
                    @NonNull String path,
                    WatchEvent.Kind<?> eventKind) throws IOException;
    }
}
