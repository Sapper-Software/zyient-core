package ai.sapper.cdc.core.sources;

import com.codekutter.common.stores.impl.DataStoreAuditContext;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MailStoreAuditContext extends DataStoreAuditContext {
    private String mailbox;
    private String folder;
}
