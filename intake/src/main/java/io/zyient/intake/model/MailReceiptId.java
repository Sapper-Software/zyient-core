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

import io.zyient.base.common.model.entity.IKey;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Getter
@Setter
@Embeddable
public class MailReceiptId implements IKey {
    @Column(name = "mail_id")
    private String mailId;
    @Column(name = "id")
    private String id;

    public MailReceiptId() {

    }

    public MailReceiptId(String mailId, String id) {
        this.mailId = mailId;
        this.id = id;
    }

    @Override
    public String stringKey() {
        return String.format("%s::%s", mailId, id);
    }

    @Override
    public int compareTo(IKey iKey) {
        if (iKey instanceof MailReceiptId) {
            int ret = mailId.compareTo(((MailReceiptId) iKey).mailId);
            if (ret == 0) {
                ret = id.compareTo(((MailReceiptId) iKey).id);
            }
            return ret;
        }
        return -1;
    }
}
