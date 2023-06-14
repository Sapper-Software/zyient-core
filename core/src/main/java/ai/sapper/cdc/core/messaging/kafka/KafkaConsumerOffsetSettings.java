package ai.sapper.cdc.core.messaging.kafka;

import ai.sapper.cdc.core.state.OffsetStateManagerSettings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

/**
 * <pre>
 *     <offsetManager>
 *         <name>[Name]</name>
 *         <type>[Class extends OffsetStateManager]</type>
 *         <connection>ZooKeeper connection name</connection>
 *         <basePath>ZooKeeper base path</basePath>
 *         <locking>
 *             -- optional
 *             <retry>[Retry count]</retry>
 *             <timeout>Lock acquire timeout</timeout>
 *         </locking>
 *     </offsetManager>
 * </pre>
 */
@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class KafkaConsumerOffsetSettings extends OffsetStateManagerSettings {
}
