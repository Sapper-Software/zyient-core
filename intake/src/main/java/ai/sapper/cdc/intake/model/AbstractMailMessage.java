package ai.sapper.cdc.intake.model;

import ai.sapper.cdc.core.model.IEntity;
import ai.sapper.cdc.core.model.StringKey;
import ai.sapper.cdc.core.utils.FileUtils;
import ai.sapper.cdc.intake.utils.MailUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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
    public StringKey getKey() {
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
