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

package io.zyient.intake.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.intake.flow.FlowTaskException;
import io.zyient.intake.model.*;
import io.zyient.base.common.GlobalConstants;
import io.zyient.base.common.utils.DefaultLogger;

import javax.annotation.Nonnull;

public class MailAuditDataUtils {

    public static MailReceiptRecord create(@Nonnull EIntakeChannel channel,
                                           @Nonnull AbstractMailMessage<?> message,
                                           @Nonnull String callerId) throws FlowTaskException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(callerId));
        Preconditions.checkArgument(message.getMessage() != null);

        try {
            MailReceiptRecord record = new MailReceiptRecord();
            record.setMessageId(new MailReceiptId(message.getMailId(), message.getMessageId().getKey()));
            record.setChannel(channel);
            record.setProcessorId(callerId);
            record.setReadTimestamp(System.currentTimeMillis());
            if (message.getHeader() == null) {
                MessageHeader header = MailUtils.parseHeader(message);
                message.setHeader(header);
            }
            record.setHeaderJson(MailUtils.generateHeaderJson(message.getHeader()));
            record.setReceivedTimestamp(message.getHeader().getReceivedDate());
            record.setMessageHash(MailUtils.generateMessageHash(MailUtils.parseHeader(message)));

            return record;
        } catch (Exception ex) {
            throw new FlowTaskException(ex);
        }
    }

    public static MailMetaDataRecord create(@Nonnull AbstractMailMessage<?> message,
                                            @Nonnull String callerId) throws FlowTaskException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(callerId));
        Preconditions.checkArgument(message.getMessage() != null);

        try {
            MailMetaDataRecord record = new MailMetaDataRecord();
            record.setMessageId(new MailReceiptId(message.getMailId(), message.getMessageId().getKey()));
            if (message.getHeader() == null) {
                MessageHeader header = MailUtils.parseHeader(message);
                message.setHeader(header);
            }
            record.setHeaderJson(MailUtils.generateHeaderJson(message.getHeader()));
            record.setReceivedTimestamp(message.getHeader().getReceivedDate());
            record.setProcessedTimestamp(System.currentTimeMillis());
            return record;
        } catch (Exception ex) {
            throw new FlowTaskException(ex);
        }
    }

    public static MessageHeader getHeader(@Nonnull MailReceiptRecord record) throws Exception {
        String json = record.getHeaderJson();
        if (Strings.isNullOrEmpty(json)) {
            throw new FlowTaskException("NULL/Empty header JSON data.");
        }
        MessageHeader header = GlobalConstants.getJsonMapper().readValue(json, MessageHeader.class);
        DefaultLogger.trace(header);

        return header;
    }
}
