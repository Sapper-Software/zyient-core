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
