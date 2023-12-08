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

package io.zyient.base.core.stores.impl.solr.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.base.common.model.entity.EEntityState;
import io.zyient.base.core.stores.model.Document;
import io.zyient.base.core.stores.model.DocumentId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@Entity
@Table(name = "tb_documents", schema = "test")
public class DemoTestDocument extends Document<DemoDocState, ReferenceKey, DemoTestDocument> {
    public DemoTestDocument() {
        super(new DemoDocState());
    }

    @Override
    public Document<DemoDocState, ReferenceKey, DemoTestDocument> createInstance() throws Exception {
        DemoTestDocument doc = new DemoTestDocument();
        DocumentId id = new DocumentId(getId().getCollection());
        doc.setId(id);
        doc.setReferenceId(getReferenceId());
        doc.setDocState(new DemoDocState());
        doc.getDocState().setState(EEntityState.New);
        doc.getState().setState(EEntityState.New);
        doc.setProperties(getProperties());
        return doc;
    }
}
