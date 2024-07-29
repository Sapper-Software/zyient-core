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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.StringKey;
import io.zyient.base.core.utils.FileUtils;
import io.zyient.intake.utils.MailUtils;
import lombok.Getter;
import lombok.Setter;

import java.io.File;

@Getter
@Setter
public abstract class AbstractMailMessage<T> implements IEntity<StringKey> {
    protected String mailId;
    protected StringKey messageId;
    protected String messageHash;
    protected MessageHeader header;
    @JsonIgnore
    protected File messageFile;
    @JsonIgnore
    protected T message;

    @Override
    public int compare(StringKey stringKey) {
        return messageId.compareTo(stringKey);
    }

    @Override
    public StringKey entityKey() {
        return messageId;
    }

    public String messageDir() {
        return FileUtils.cleanDirName(getMessageId().stringKey());
    }
    
    public void load() throws Exception {
    	Preconditions.checkState(message != null);
    	if (header == null) {
    		header = MailUtils.parseHeader(this);
    	}
    	if (Strings.isNullOrEmpty(messageHash)) {
    		messageHash = MailUtils.generateMessageHash(header);
    	}
    }
}
