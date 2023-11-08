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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.utils.ReflectionUtils;
import io.zyient.base.core.io.model.FileInode;
import io.zyient.base.core.model.BaseEntity;
import io.zyient.base.core.utils.FileUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import javax.persistence.*;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Entity
@Table(name = "ingest_file_records")
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class FileItemRecord extends BaseEntity<IdKey> {
    @Id
    @Column(name = "id")
    private IdKey fileId;
    @Column(name = "intake_channel")
    @Enumerated(EnumType.STRING)
    private EIntakeChannel channel;
    @Column(name = "record_type")
    @Enumerated(EnumType.STRING)
    private EFileRecordType recordType;
    @Column(name = "drive")
    private String drive;
    @Column(name = "source_folder")
    private String sourceFolder;
    @Column(name = "name")
    private String fileName;
    @Column(name = "file_location")
    private Map<String, String> fileLocation;
    @Column(name = "file_location_url")
    private String fileLocationUrl;
    @Column(name = "processed_timestamp")
    private long processedTimestamp;
    @Column(name = "read_timestamp")
    private long readTimestamp;
    @Column(name = "processing_timestamp")
    private long processingTimestamp;
    @Column(name = "reprocess_count")
    private int reProcessCount;
    @Column(name = "source_user_id")
    private String sourceUserId;
    @Column(name = "processing_status")
    @Enumerated(EnumType.STRING)
    private ERecordState state = ERecordState.Unknown;
    @Column(name = "error_message")
    private String errorMessage;
    @Column(name = "file_type")
    private String fileType;
    @Column(name = "file_size")
    private long fileSize;
    @Column(name = "parent_id")
    private String parentId;
    @Column(name = "context_json")
    private String contextJson;
    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
    @JoinColumn(name = "parent_id")
    @Reference(target = FileItemRecord.class, reference = "itemReferences")
    private Set<FileItemRecord> fileItemRecords;
    @Column(name = "request_id")
    private String requestId;
    @Embedded
    private RecordReference recordReference;
    @Version
    @Column(name = "record_version")
    private long recordVersion = 0;
    @Column(name = "is_inline_attachment")
    private boolean isInlineAttachment = false;
    @Transient
    private UpdateParams updateParams;
    @Transient
    private JsonReference itemReferences;

    public FileItemRecord() {
    }

    public boolean hasError() {
        return state == ERecordState.Error;
    }

    public void addFileItemRecord(@NonNull FileItemRecord record) {
        if (fileItemRecords == null) {
            fileItemRecords = new HashSet<>();
        }
        record.setParentId(fileId.getId());

        fileItemRecords.add(record);
    }

    public static FileItemRecord create(@NonNull EIntakeChannel channel,
                                        @NonNull FileInode inode,
                                        @NonNull String userId) throws IOException {
        try {
            FileItemRecord record = new FileItemRecord();
            // TODO: File ID shouldn't be random
            record.fileId = new IdKey(inode.getUuid());
            record.channel = channel;
            record.setDrive(inode.getDomain());
            record.setParentId(inode.getParent().getUuid());
            record.fileName = inode.getName();
            record.fileLocation = inode.getPath();
            record.fileType = FileUtils.getFileMimeType(inode.getFsPath());
            record.fileSize = inode.size();
            record.setState(ERecordState.Unknown);
            record.processingTimestamp = System.currentTimeMillis();
            record.readTimestamp = System.currentTimeMillis();
            record.setSourceUserId(userId);

            return record;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public int compare(IdKey idKey) {
        return 0;
    }

    @Override
    public IEntity<IdKey> copyChanges(IEntity<IdKey> iEntity,
                                      Context context) throws CopyException {
        try {
            if (iEntity instanceof FileItemRecord source) {
                this.fileId = source.fileId;
                this.fileItemRecords = new HashSet<>(source.fileItemRecords);
                this.fileLocation = source.fileLocation;
                this.recordReference = source.recordReference;
                this.updateParams = source.updateParams;
                ReflectionUtils.copyNatives(source, this);
            } else {
                throw new CopyException(String.format("Invalid entity: [type=%s]", iEntity.getClass().getCanonicalName()));
            }
            return this;
        } catch (Exception ex) {
            throw new CopyException(ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public IEntity<IdKey> clone(Context context) throws CopyException {
        try {
            return (IEntity<IdKey>) clone();
        } catch (Exception ex) {
            throw new CopyException(ex);
        }
    }

    /**
     * Validate this entity instance.
     *
     * @throws ValidationExceptions - On validation failure will throw exception.
     */
    @Override
    public void doValidate() throws ValidationExceptions {

    }

    @Override
    @JsonIgnore
    public IdKey entityKey() {
        return fileId;
    }
}
