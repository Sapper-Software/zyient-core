package ai.sapper.cdc.core.messaging.builders;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.connections.settings.EConnectionType;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

/**
 * <pre>
 *     <receiver> -- Or root name
 *         <type>[EConnectionType]</type>
 *         <connection>[Message Connection name]</connection>
 *         <offset>
 *             <manager>[Offset Manager name]</manager>
 *         </offset>
 *         <batchSize>[Receive batch size, default = -1(ignore)]</batchSize>
 *         <receiverTimeout>[Receiver timeout, default = -1(ignore)]</receiverTimeout>
 *     </receiver>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class MessageReceiverSettings extends Settings {
    @Config(name = "type", type = Enum.class)
    private EConnectionType type;
    @Config(name = "connection")
    private String connection;
    @Config(name = "offset.manager", required = false)
    private String offsetManager;
    @Config(name = "batchSize", required = false, type = Integer.class)
    private int batchSize = -1;
    @Config(name = "receiverTimeout", required = false, type = Long.class)
    private long receiverTimeout = -1;
}
