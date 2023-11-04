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
import com.google.common.base.Strings;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
@Entity
@Table(name = "config_mail_groups")
public class MailGroupEntity implements IEntity<IdKey> {
    @EmbeddedId
    private IdKey id;
    @Transient
    @Setter(AccessLevel.NONE)
    private Set<String> recipients;
    @Column(name = "recipients")
    @Setter(AccessLevel.NONE)
    @JsonIgnore
    private String recipientList;

    public void setRecipientList(String recipientList) {
        this.recipientList = recipientList;
        if (!Strings.isNullOrEmpty(recipientList)) {
            recipients = new HashSet<>();
            String[] parts = recipientList.split(";");
            if (parts != null && parts.length > 0) {
                recipients.addAll(Arrays.asList(parts));
            }
        } else {
            recipients = null;
        }
    }

    public void setRecipients(Set<String> recipients) {
        this.recipients = recipients;
        if (recipients != null) {
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            for (String r : recipients) {
                if (first) first = false;
                else {
                    builder.append(";");
                }
                builder.append(r);
            }
            recipientList = builder.toString();
        }
    }

    public void addRecipient(@Nonnull String recipient) {
        if (recipients == null) {
            recipients = new HashSet<>();
        }
        recipients.add(recipient);
        if (!Strings.isNullOrEmpty(recipientList)) {
            recipientList = String.format("%s;%s", recipientList, recipient);
        } else
            recipientList = recipient;
    }

    public boolean removeRecipient(@Nonnull String recipient) {
        if (recipients != null) {
            boolean ret = recipients.remove(recipient);
            if (ret) {
                StringBuilder builder = new StringBuilder();
                boolean first = true;
                for (String r : recipients) {
                    if (first) first = false;
                    else {
                        builder.append(";");
                    }
                    builder.append(r);
                }
                recipientList = builder.toString();
            }
            return ret;
        }
        return false;
    }

    public void clearRecipients() {
        if (recipients != null) recipients.clear();
        recipientList = null;
    }

    @Override
    public int compare(IdKey idKey) {
        return id.compareTo(idKey);
    }

    @Override
    public IEntity<IdKey> copyChanges(IEntity<IdKey> iEntity, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<IdKey> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public IdKey entityKey() {
        return id;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
