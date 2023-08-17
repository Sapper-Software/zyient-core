package ai.sapper.cdc.intake.model;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

import javax.persistence.*;

@Entity
@Table(name = "ingest_mail_receipts")
@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class MailReceiptRecord implements IEntity<MailReceiptId> {
    @EmbeddedId
    private MailReceiptId messageId;
    @Column(name = "state")
    @Enumerated(EnumType.STRING)
    private ERecordState state = ERecordState.Unknown;
    @Column(name = "intake_channel")
    @Enumerated(EnumType.STRING)
    private EIntakeChannel channel;
    @Column(name = "message_hash")
    private String messageHash;
    @Column(name = "header_json")
    private String headerJson;
    @Column(name = "received_timestamp")
    private long receivedTimestamp;
    @Column(name = "read_timestamp")
    private long readTimestamp;
    @Column(name = "processed_timestamp")
    private long processedTimestamp;
    @Column(name = "reprocess_count")
    private int reProcessCount;
    @Column(name = "attachment_count")
    private int attachmentCount;
    @Column(name = "error_message")
    private String errorMessage;
    @Column(name = "context_json")
    private String contextJson;
    @Column(name = "processor_id")
    private String processorId;
    @Column(name = "processing_timestamp")
    private long processingTimestamp;
    @Column(name = "is_literature")
    private boolean literature;
    @OneToOne(cascade = CascadeType.ALL, orphanRemoval = true)
    @JoinColumn(name = "file_record_id")
    private FileItemRecord fileItemRecord;
    @Version
    @Column(name = "record_version")
    private long recordVersion = 0;
    @Transient
    @JsonIgnore
    private Throwable error;

    public MailReceiptRecord() {
        this.channel = EIntakeChannel.Email;
    }

    public boolean hasError() {
        return state == ERecordState.Error;
    }



    @Override
    public void validate() throws ValidationExceptions {

    }

    @Override
    public int compare(MailReceiptId mailReceiptId) {
        return messageId.compareTo(mailReceiptId);
    }

    @Override
    public IEntity<MailReceiptId> copyChanges(IEntity<MailReceiptId> iEntity, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<MailReceiptId> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    @JsonIgnore
    public MailReceiptId getKey() {
        return messageId;
    }
}
