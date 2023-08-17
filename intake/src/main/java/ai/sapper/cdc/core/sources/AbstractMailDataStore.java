package ai.sapper.cdc.core.sources;

import ai.sapper.cdc.core.stores.AbstractDataStore;
import ai.sapper.cdc.core.stores.DataStoreException;
import ai.sapper.cdc.intake.model.AbstractMailMessage;
import ai.sapper.cdc.intake.model.EmailMessage;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public abstract class AbstractMailDataStore<T, M, F> extends AbstractDataStore<T> {
    protected String mailbox;

    public abstract String getMailUser();

    public abstract M createMessage(@Nonnull String mailId,
                                    @Nonnull EmailMessage email,
                                    @Nonnull String sender) throws DataStoreException;

    public abstract AbstractMailMessage<M> createWrappedMessage(@Nonnull String mailI,
                                                                @Nonnull EmailMessage email,
                                                                @Nonnull String sender) throws DataStoreException;

    public abstract AbstractMailMessage<M> createMessage(@Nonnull String mailI,
                                                         @Nonnull String sender,
                                                         @Nonnull String[] sendTo,
                                                         String[] ccTo,
                                                         String[] bccTo,
                                                         String subject) throws DataStoreException;

    public abstract boolean isEmpty(@Nonnull String folder) throws DataStoreException;

    public abstract boolean isEmpty(@Nonnull F folder) throws DataStoreException;

    public abstract F getFolder(@Nonnull String name) throws DataStoreException;

    public abstract F createFolder(@Nonnull String name) throws DataStoreException;

    public abstract List<AbstractMailMessage<M>> recover(String folder, boolean deleteEmpty) throws DataStoreException;


    public abstract Collection<AbstractMailMessage<M>> fetch(String folderName,
                                                             int batchSize,
                                                             int offset) throws DataStoreException;

    public abstract void move(@Nonnull AbstractMailMessage<M> message, @Nonnull String targetf) throws DataStoreException;


    public abstract void move(@Nonnull AbstractMailMessage<M>[] messages, @Nonnull String targetf) throws DataStoreException;

    public abstract AbstractMailMessage<M> readFromFile(@Nonnull String path) throws DataStoreException;

    public abstract void deleteChildFolders(@Nonnull String path) throws DataStoreException;

    public abstract boolean deleteFolder(@Nonnull String path, boolean recursive) throws DataStoreException;

    public abstract List<String> folders(String term) throws DataStoreException;
    
    public abstract Map<String, Integer> getFolderMessageCounts(String parent) throws DataStoreException;

    public abstract void cleanUpFolders(String parent) throws DataStoreException;

    public abstract void cleanUpEmptyFoldersBasedOnTimePeriod(String processingFolderPath, String folderPath, int timePeriod) throws DataStoreException;
}
