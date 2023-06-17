package ai.sapper.cdc.core.messaging.builders;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.connections.settings.EConnectionType;
import lombok.Getter;
import lombok.Setter;

/**
 * <pre>
 *     <type>[EConnectionType]</type>
 *     <connection>[Message connection name]</connection>
 * </pre>
 */
@Getter
@Setter
public class MessageSenderSettings extends Settings {
    @Config(name = "type", type = EConnectionType.class)
    private EConnectionType type;
    @Config(name = "connection")
    private String connection;
}
