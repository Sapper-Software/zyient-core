package ai.sapper.cdc.intake.model;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

@Entity
@Table(name = "ingest_mail_metadata")
@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class MailMetaDataRecord implements IEntity<MailReceiptId> {
    @EmbeddedId
    private MailReceiptId messageId;
    @Column(name = "header_json")
    private String headerJson;
    @Column(name = "received_timestamp")
    private long receivedTimestamp;
    @Column(name = "processed_timestamp")
    private long processedTimestamp;
    @Transient
    @JsonIgnore
    private Throwable error;

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
