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

import jakarta.mail.Message;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class EmailMessage {
    private MessageHeader header;
    private String body;
    private List<String> attachments;
    private String auditedEMLFile;

    public void addAttachment(@Nonnull File file) throws IOException {
        if (attachments == null) {
            attachments = new ArrayList<>();
        }
        attachments.add(file.getAbsolutePath());
    }

    public void addRecipient(@Nonnull String email, Message.RecipientType type) throws AddressException {
        InternetAddress address = new InternetAddress(email);
        header.addRecipient(address, type);
    }

    public void addReplyTo(@Nonnull String email) throws AddressException {
        InternetAddress address = new InternetAddress(email);
        header.addReplyTo(address);
    }

    public void addFrom(@Nonnull String email) throws AddressException {
        InternetAddress address = new InternetAddress(email);
        header.addFrom(address);
    }
}
