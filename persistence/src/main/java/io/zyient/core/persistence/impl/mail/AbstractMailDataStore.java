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

package io.zyient.core.persistence.impl.mail;

import io.zyient.base.core.stores.AbstractDataStore;
import io.zyient.base.core.stores.DataStoreException;
import io.zyient.core.persistence.AbstractDataStore;
import io.zyient.core.persistence.DataStoreException;
import io.zyient.intake.model.AbstractMailMessage;
import io.zyient.intake.model.EmailMessage;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import microsoft.exchange.webservices.data.core.service.item.EmailMessage;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class AbstractMailDataStore<T, M, F> extends AbstractDataStore<T> {
    protected String mailbox;

    public abstract String getMailUser();

    public abstract M createMessage(@NonNull String mailId,
                                    @NonNull EmailMessage email,
                                    @NonNull String sender) throws DataStoreException;

    public abstract AbstractMailMessage<M> createWrappedMessage(@NonNull String mailI,
                                                                @NonNull EmailMessage email,
                                                                @NonNull String sender) throws DataStoreException;

    public abstract AbstractMailMessage<M> createMessage(@NonNull String mailI,
                                                         @NonNull String sender,
                                                         @NonNull String[] sendTo,
                                                         String[] ccTo,
                                                         String[] bccTo,
                                                         String subject) throws DataStoreException;

    public abstract boolean isEmpty(@NonNull String folder) throws DataStoreException;

    public abstract boolean isEmpty(@NonNull F folder) throws DataStoreException;

    public abstract F getFolder(@NonNull String name) throws DataStoreException;

    public abstract F createFolder(@NonNull String name) throws DataStoreException;

    public abstract List<AbstractMailMessage<M>> recover(String folder, boolean deleteEmpty) throws DataStoreException;


    public abstract Collection<AbstractMailMessage<M>> fetch(String folderName,
                                                             int batchSize,
                                                             int offset) throws DataStoreException;

    public abstract void move(@NonNull AbstractMailMessage<M> message, @NonNull String targetf) throws DataStoreException;


    public abstract void move(@NonNull AbstractMailMessage<M>[] messages, @NonNull String targetf) throws DataStoreException;

    public abstract AbstractMailMessage<M> readFromFile(@NonNull String path) throws DataStoreException;

    public abstract void deleteChildFolders(@NonNull String path) throws DataStoreException;

    public abstract boolean deleteFolder(@NonNull String path, boolean recursive) throws DataStoreException;

    public abstract List<String> folders(String term) throws DataStoreException;
    
    public abstract Map<String, Integer> getFolderMessageCounts(String parent) throws DataStoreException;

    public abstract void cleanUpFolders(String parent) throws DataStoreException;

    public abstract void cleanUpEmptyFoldersBasedOnTimePeriod(String processingFolderPath, 
                                                              String folderPath, 
                                                              int timePeriod) throws DataStoreException;
}
