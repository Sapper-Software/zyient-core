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

package io.zyient.intake.model;

import io.zyient.base.common.model.entity.EEntityState;
import lombok.Data;

@Data
public class MailProcessedMessage {
    private EEntityState state = EEntityState.Unknown;
    private String messageId;
    private String headerJson;
    private String mailUser;
    private String mailBox;
    private long readTimestamp;
    private long processedTimestamp;
    private String s3Bucket;
    private String s3folderPath;
    private EIntakeChannel channel;
    private boolean isValidLiterature = false;
    private FileItemRecord messageFile;
}
