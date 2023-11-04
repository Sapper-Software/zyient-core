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

import javax.persistence.*;
import java.util.Set;

@Getter
@Setter
@Entity
@Table(name = "config_mail_templates")
public class MailTemplate implements IEntity<IdKey> {
    @EmbeddedId
    private IdKey id;
    @Column(name = "template")
    private String template;
    @Enumerated(EnumType.STRING)
    @Column(name = "template_type")
    private ETemplateType templateType;
    @Enumerated(EnumType.STRING)
    @Column(name = "template_source")
    private EContentSource templateSource;
    @Column(name = "updated_by")
    private String updatedBy;
    @Column(name = "update_timestamp")
    private long updateTimestamp;
    @OneToMany(fetch = FetchType.EAGER)
    @JoinColumn(name = "template_id")
    private Set<MailTemplateParam> params;


    @Override
    public int compare(IdKey idKey) {
        return id.compareTo(idKey);
    }

    @Override
    public IEntity<IdKey> copyChanges(IEntity<IdKey> iEntity, Context context) throws CopyException {
        throw new CopyException("Method not implemented.");
    }

    @Override
    public IEntity<IdKey> clone(Context context) throws CopyException {
        throw new CopyException("Method not implemented.");
    }

    @Override
    public IdKey entityKey() {
        return id;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
