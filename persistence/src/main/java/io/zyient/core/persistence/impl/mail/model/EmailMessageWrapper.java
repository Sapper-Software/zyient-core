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

package io.zyient.core.persistence.impl.mail.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.StringKey;
import io.zyient.core.persistence.impl.mail.MailUtils;
import io.zyient.intake.utils.MailUtils;
import lombok.Getter;
import lombok.Setter;
import microsoft.exchange.webservices.data.core.service.item.EmailMessage;
import microsoft.exchange.webservices.data.property.complex.MessageBody;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.UUID;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class EmailMessageWrapper extends AbstractMailMessage<EmailMessage> {
    public static final String EMAIL_CORRELATION_ID = "pvaiCorrelationId";
    private String pvaiCorrelationId;

    public EmailMessageWrapper() {

    }

    public EmailMessageWrapper(@Nonnull String mailId,
                               @Nonnull EmailMessage message,
                               boolean parse) {
        try {
            this.mailId = mailId;
            this.message = message;
            if (message.getId() != null) {
                this.messageId = new StringKey(message.getInternetMessageId());
                if (messageId == null || Strings.isNullOrEmpty(messageId.getKey())) {
                    messageId = new StringKey(generateMessageKey(this));
                }
                messageHash = generateMessageKey(this);
            } else {
                this.messageId = new StringKey(UUID.randomUUID().toString());
            }
            if (parse) {
                header = MailUtils.parseHeader(this);
                messageHash = MailUtils.generateMessageHash(header);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public IEntity<StringKey> copyChanges(IEntity<StringKey> iEntity, Context context) throws CopyException {
        throw new CopyException("Not implemented...");
    }

    @Override
    public IEntity<StringKey> clone(Context context) throws CopyException {
        throw new CopyException("Not implemented...");
    }

    @Override
    public void validate() throws ValidationExceptions {

    }

    @JsonIgnore
    public EmailMessageWrapper setText(@Nonnull String text) throws Exception {
        MessageBody body = MessageBody.getMessageBodyFromText(text);
        message.setBody(body);
        return this;
    }

    public EmailMessageWrapper addAttachment(@Nonnull String filename) throws Exception {
        File file = new File(filename);
        if (!file.exists()) {
            throw new Exception(String.format("File not found. [path=%s]", file.getAbsolutePath()));
        }
        message.getAttachments().addFileAttachment(file.getAbsolutePath());
        return this;
    }

    public static String generateMessageKey(@Nonnull EmailMessageWrapper message) throws Exception {
        Preconditions.checkArgument(message != null);
        MessageHeader header = MailUtils.parseHeader(message);
        return MailUtils.generateMessageHash(header);
    }
}
