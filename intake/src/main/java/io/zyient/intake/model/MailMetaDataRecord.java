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
