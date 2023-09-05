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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.model.StringKey;
import io.zyient.intake.utils.MailUtils;
import jakarta.activation.DataHandler;
import jakarta.activation.DataSource;
import jakarta.activation.FileDataSource;
import jakarta.mail.BodyPart;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMultipart;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.io.File;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class MessageWrapper extends AbstractMailMessage<Message> {
    public static final String PARAM_MESSAGE_ID = "Message-ID";
    public static final String PARAM_AWS_MESSAGE_ID = "Message-ID";
    public static final String CONTEXT_KEY_SESSION = "Mail.Session";

    @JsonIgnore
    private Multipart multipart = new MimeMultipart();

    public MessageWrapper() {

    }

    public MessageWrapper(@NonNull String mailId, @NonNull Message message) {
        this.mailId = mailId;
        this.message = message;
        try {
            String[] arr = message.getHeader(PARAM_MESSAGE_ID);
            if (arr == null || arr.length <= 0) {
                arr = message.getHeader(PARAM_AWS_MESSAGE_ID);
            }
            if (arr != null && arr.length > 0) {
                messageId = new StringKey(arr[0]);
            }
            /*
            if (messageId == null || Strings.isNullOrEmpty(messageId.getKey())) {
                messageId = new StringKey(generateMessageKey(this));
            }
            */

            header = MailUtils.parseHeader(this);
            messageHash = MailUtils.generateMessageHash(header);
        } catch (Exception ex) {
            DefaultLogger.error(ex.getLocalizedMessage());
            throw new RuntimeException(ex);
        }
    }

    public MessageWrapper(@NonNull String mailId, @NonNull Message message, boolean generateId) {
        this.mailId = mailId;
        this.message = message;
        try {
            String[] arr = message.getHeader(PARAM_MESSAGE_ID);
            DefaultLogger.info(
                    String.format(" MessageWrapper [mailId%s]", mailId));
            if (arr == null || arr.length <= 0) {
                arr = message.getHeader(PARAM_AWS_MESSAGE_ID);
            }
            if (arr != null && arr.length > 0) {
                messageId = new StringKey(arr[0]);
            }
            DefaultLogger.info(
                    String.format(" MessageWrapper [messageId%s]", messageId));
            if (messageId == null || Strings.isNullOrEmpty(messageId.getKey())) {
                if (generateId)
                    messageId = new StringKey(generateMessageKey(this));
            }

            header = MailUtils.parseHeader(this);
            DefaultLogger.debug(
                    String.format("  MessageWrapper After parseHeader  Successfull [header%s]", header));
            messageHash = MailUtils.generateMessageHash(header);
        } catch (Exception ex) {
            DefaultLogger.error(ex.getLocalizedMessage());
            throw new RuntimeException(ex);
        }
    }

    public MessageWrapper attach(String filename) throws MessagingException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(filename));
        File file = new File(filename);
        if (!file.exists()) {
            throw new MessagingException(String.format("File not found. [file=%s]",
                    file.getAbsolutePath()));
        }
        BodyPart messageBodyPart = new MimeBodyPart();

        DataSource source = new FileDataSource(filename);
        messageBodyPart.setDataHandler(new DataHandler(source));
        messageBodyPart.setFileName(file.getName());
        multipart.addBodyPart(messageBodyPart);

        return this;
    }

    public MessageWrapper setText(@NonNull String text) throws MessagingException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(text));

        // Create the message part
        BodyPart messageBodyPart = new MimeBodyPart();
        // Now set the actual message
        messageBodyPart.setText(text);
        multipart.addBodyPart(messageBodyPart);

        return this;
    }

    public MessageWrapper finish() throws MessagingException {
        message.setContent(multipart);
        return this;
    }

    public static String generateMessageKey(@NonNull MessageWrapper message) throws Exception {
        Preconditions.checkArgument(message != null);
        MessageHeader header = MailUtils.parseHeader(message);
        return MailUtils.generateMessageHash(header);
    }

    @Override
    public IEntity<StringKey> copyChanges(IEntity<StringKey> entity, Context context) throws CopyException {
        throw new CopyException("Not implemented...");
    }

    @Override
    public IEntity<StringKey> clone(Context context) throws CopyException {
        throw new CopyException("Not implemented...");
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
