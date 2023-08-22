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

package ai.sapper.cdc.core.io.model;

import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.utils.MetricsBase;
import io.micrometer.core.instrument.DistributionSummary;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public class FileSystemMetrics extends MetricsBase {
    public static final String METRICS_CREATE_DIR = "dir_create";
    public static final String METRICS_DELETE_DIR = "dir_delete";
    public static final String METRICS_CREATE_FILE = "file_create";
    public static final String METRICS_UPDATE_FILE = "file_update";
    public static final String METRICS_DELETE_FILE = "file_delete";
    public static final String METRICS_WRITE_FILE = "file_write";
    public static final String METRICS_READ_FILE = "file_read";
    public static final String METRICS_WRITE_OPEN_FILE = "file_write_open";
    public static final String METRICS_READ_OPEN_FILE = "file_read_open";
    public static final String METRICS_WRITE_COMMIT_FILE = "file_write_commit";

    private final DistributionSummary timerDirCreate;
    private final DistributionSummary timerDirDelete;
    private final DistributionSummary timerFileCreate;
    private final DistributionSummary timerFileUpdate;
    private final DistributionSummary timerFileDelete;
    private final DistributionSummary timerFileWrite;
    private final DistributionSummary timerFileRead;
    private final DistributionSummary timerFileWriteOpen;
    private final DistributionSummary timerFileReadOpen;
    private final DistributionSummary timerFileWriteCommit;

    public FileSystemMetrics(@NonNull String engine,
                             @NonNull String name,
                             @NonNull String sourceType,
                             @NonNull BaseEnv<?> env) {
        super(engine, name, sourceType, env);
        timerDirCreate = addTimer(METRICS_CREATE_DIR, null);
        timerDirDelete = addTimer(METRICS_DELETE_DIR, null);
        timerFileCreate = addTimer(METRICS_CREATE_FILE, null);
        timerFileUpdate = addTimer(METRICS_UPDATE_FILE, null);
        timerFileDelete = addTimer(METRICS_DELETE_FILE, null);
        timerFileWrite = addTimer(METRICS_WRITE_FILE, null);
        timerFileWriteOpen = addTimer(METRICS_WRITE_OPEN_FILE, null);
        timerFileWriteCommit = addTimer(METRICS_WRITE_COMMIT_FILE, null);
        timerFileRead = addTimer(METRICS_READ_FILE, null);
        timerFileReadOpen = addTimer(METRICS_READ_OPEN_FILE, null);
    }
}
