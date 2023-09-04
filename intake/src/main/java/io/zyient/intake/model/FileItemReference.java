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

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;

@Getter
@Setter
@Entity
@Table(name = "ingest_file_reference")
public class FileItemReference implements IEntity<FileItemKey> {
    @EmbeddedId
    private FileItemKey key;
    @Column(name = "reference_type")
    private String referenceType;
    @Column(name = "reference_key")
    private String referenceKey;
    @Column(name = "encoding_key")
    private String encodingKey;
    @Column(name = "request_id")
    private String requestId;
    @Column(name = "status")
    @Enumerated(EnumType.STRING)
    private ERecordState state = ERecordState.Unknown;
    @Column(name = "error_message")
    private String error;

    @Override
    public void validate() throws ValidationExceptions {

    }

    @Override
    public int compare(FileItemKey fileItemKey) {
        return key.compareTo(fileItemKey);
    }

    @Override
    public IEntity<FileItemKey> copyChanges(IEntity<FileItemKey> iEntity, Context context) throws CopyException {
        throw new CopyException("Method not implemented.");
    }

    @Override
    public IEntity<FileItemKey> clone(Context context) throws CopyException {
        throw new CopyException("Method not implemented.");
    }

    @Override
    public FileItemKey getKey() {
        return key;
    }
}
