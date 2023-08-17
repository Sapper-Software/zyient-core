package ai.sapper.cdc.intake.model;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
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
