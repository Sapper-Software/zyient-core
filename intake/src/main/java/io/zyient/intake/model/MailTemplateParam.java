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

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Getter
@Setter
@Entity
@Table(name = "config_mail_template_params")
public class MailTemplateParam implements IEntity<MailTemplateParamId> {
    @EmbeddedId
    private MailTemplateParamId id;
    @Column(name = "is_mandatory")
    private boolean mandatory;
    @Column(name = "default_value")
    private String defaultValue;

    @Override
    public int compare(MailTemplateParamId mailTemplateParamId) {
        return id.compareTo(mailTemplateParamId);
    }

    @Override
    public IEntity<MailTemplateParamId> copyChanges(IEntity<MailTemplateParamId> iEntity, Context context) throws CopyException {
        throw new CopyException("Method not implemented.");
    }

    @Override
    public IEntity<MailTemplateParamId> clone(Context context) throws CopyException {
        throw new CopyException("Method not implemented.");
    }

    @Override
    public MailTemplateParamId getKey() {
        return id;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
