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

import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class EmailJsonWrapper {
    private String senderName;
    private String senderEmail;
    private MessageHeaderWrapper header;
    private List<String> toGroups;
    private List<String> ccGroups;
    private List<String> bccGroups;
    private List<BaseAttachmentWrapper> attachments;
    private String template;
    private Map<String, String> context;
    private String body;

    public void addAttachment(@Nonnull BaseAttachmentWrapper attachment) {
        if (attachments == null) {
            attachments = new ArrayList<>();
        }
        attachments.add(attachment);
    }

    public void addToGroup(@Nonnull String mailGroup) {
        if (toGroups == null) {
            toGroups = new ArrayList<>();
        }
        toGroups.add(mailGroup);
    }

    public void addCcGroup(@Nonnull String mailGroup) {
        if (ccGroups == null) {
            ccGroups = new ArrayList<>();
        }
        ccGroups.add(mailGroup);
    }
    public void addBccGroup(@Nonnull String mailGroup) {
        if (bccGroups == null) {
            bccGroups = new ArrayList<>();
        }
        bccGroups.add(mailGroup);
    }
}
