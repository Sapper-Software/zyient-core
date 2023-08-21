package ai.sapper.cdc.intake.model;

import ai.sapper.cdc.common.model.Context;
import ai.sapper.cdc.common.model.CopyException;
import ai.sapper.cdc.common.model.ValidationExceptions;
import ai.sapper.cdc.common.model.entity.IEntity;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.utils.FileUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.*;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Entity
@Table(name = "ingest_file_records")
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class FileItemRecord implements IEntity<IdKey> {
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
    private String fileLocation;
    @Column(name = "file_location_url")
    private String fileLocationUrl;
    @Column(name = "file_pdf_location_url")
    private String filePdfLocationUrl;
    @Column(name = "checksum")
    private String checksum;
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
    private ERecordState state = null;
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
    private Set<FileItemRecord> fileItemRecords;
    @Column(name = "request_id")
    private String requestId;
    @Embedded
    private RecordReference recordReference;
    @Version
    @Column(name = "record_version")
    private long recordVersion = 0;
    @Transient
    @JsonIgnore
    private File path;
    @Transient
    @JsonIgnore
    private Throwable error;
    @Column(name = "is_valid_literature")
    private boolean isValidLiterature = false;
    @Column(name = "is_inline_attachment")
    private boolean isInlineAttachment = false;
    @Transient
    private UpdateParams updateParams;

    public FileItemRecord() {
        state = ERecordState.Unknown;
    }

    public boolean hasError() {
        return state == ERecordState.Error;
    }

    public void addFileItemRecord(@Nonnull FileItemRecord record) {
        if (fileItemRecords == null) {
            fileItemRecords = new HashSet<>();
        }
        record.setParentId(fileId.getId());

        fileItemRecords.add(record);
    }

    public static FileItemRecord create(@Nonnull EIntakeChannel channel,
                                        @Nonnull FileInode file,
                                        @Nonnull String drive,
                                        @Nonnull String userId,
                                        String parentId) throws IOException {
        try {
            FileItemRecord record = new FileItemRecord();
            // TODO: File ID shouldn't be random
            record.fileId = new IdKey(generateFileId(file));
            record.setChannel(channel);
            record.setDrive(drive);
            record.setParentId(parentId);
            record.fileName = file.getName();
            record.fileLocation = file.getRemotePath();
            record.checksum = FileUtils.getFileChecksum(file);
            record.fileType = FileUtils.getFileMimeType(file.getCanonicalPath());
            record.fileSize = file.size();
            record.setState(ERecordState.Unknown);
            record.processingTimestamp = System.currentTimeMillis();
            record.readTimestamp = System.currentTimeMillis();
            record.path = file;
            record.setSourceUserId(userId);
            if (file instanceof IntakeS3FileEntity) {
                record.setSourceFolder(((IntakeS3FileEntity) file).getSourceFolder());
            }
            return record;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public static FileItemRecord create(@Nonnull EIntakeChannel channel,
                                        @Nonnull File localFile,
                                        @Nonnull String drive,
                                        @Nonnull String userId,
                                        String parentId,
                                        String uploadedPath) throws IOException {
        Preconditions.checkArgument(localFile.exists());
        try {
            FileItemRecord record = new FileItemRecord();
            // TODO: File ID shouldn't be random
            record.fileId = new IdKey(generateFileId(localFile));
            record.channel = channel;
            record.setDrive(drive);
            record.setParentId(parentId);
            record.fileName = localFile.getName();
            record.fileLocation = uploadedPath;
            record.checksum = FileUtils.getFileChecksum(localFile);
            record.fileType = FileUtils.getFileMimeType(localFile.getCanonicalPath());
            record.fileSize = localFile.length();
            record.setState(ERecordState.Unknown);
            record.processingTimestamp = System.currentTimeMillis();
            record.readTimestamp = System.currentTimeMillis();
            record.path = localFile;
            record.setSourceUserId(userId);

            return record;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public static FileItemRecord create(@Nonnull EIntakeChannel channel,
                                        @Nonnull File localFile,
                                        @Nonnull String drive,
                                        @Nonnull String userId,
                                        String parentId) throws IOException {
        try {
            FileItemRecord record = create(channel, localFile, drive, userId, parentId, null);
            record.fileId = new IdKey(generateFileId(localFile));
            return record;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    public static String generateFileId(@Nonnull File file) throws IOException {
        return String.format("{%s}:{%s}", UUID.randomUUID().toString(), file.getName());
    }

    @Override
    public int compare(IdKey idKey) {
        return 0;
    }

    @Override
    public IEntity<IdKey> copyChanges(IEntity<IdKey> iEntity, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<IdKey> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }

    @Override
    @JsonIgnore
    public IdKey getKey() {
        return fileId;
    }

    public void markAsValidLiterature() {
        isValidLiterature = true;
        if (fileItemRecords != null && !fileItemRecords.isEmpty()) {
            for (FileItemRecord record : fileItemRecords) {
                record.markAsValidLiterature();
            }
        }
    }
}
