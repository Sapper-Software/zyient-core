package ai.sapper.cdc.core.state;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import ai.sapper.cdc.core.model.ESettingsSource;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public abstract class OffsetStateManagerSettings extends Settings {
    public static final String __CONFIG_PATH = "manager";

    public static final class Constants {
        public static final short LOCK_RETRY_COUNT = 4;
        public static final long LOCK_TIMEOUT = 10000;

        public static final String CONFIG_NAME = "name";
        public static final String CONFIG_TYPE = "type";
        public static final String CONFIG_ZK_BASE = "basePath";
        public static final String CONFIG_ZK_CONNECTION = "connection";
        public static final String CONFIG_LOCK_RETRY = "locking.retry";
        public static final String CONFIG_LOCK_TIMEOUT = "locking.timeout";
    }

    @Config(name = Constants.CONFIG_NAME)
    private String name;
    @Config(name = Constants.CONFIG_TYPE)
    private String type;
    @Config(name = Constants.CONFIG_ZK_BASE)
    private String basePath;
    @Config(name = Constants.CONFIG_ZK_CONNECTION)
    private String zkConnection;
    @Config(name = Constants.CONFIG_LOCK_RETRY, required = false, type = Short.class)
    private short lockRetryCount = Constants.LOCK_RETRY_COUNT;
    @Config(name = Constants.CONFIG_LOCK_TIMEOUT, required = false, type = Long.class)
    private long lockTimeout = Constants.LOCK_TIMEOUT;
    @JsonIgnore
    private ESettingsSource source;

    public OffsetStateManagerSettings() {
        source = ESettingsSource.File;
    }

    public OffsetStateManagerSettings(@NonNull Settings source) {
        super(source);
        Preconditions.checkArgument(source instanceof OffsetStateManagerSettings);
        this.name = ((OffsetStateManagerSettings) source).name;
        this.type = ((OffsetStateManagerSettings) source).type;
        this.basePath = ((OffsetStateManagerSettings) source).basePath;
        this.zkConnection = ((OffsetStateManagerSettings) source).zkConnection;
        this.lockTimeout = ((OffsetStateManagerSettings) source).lockTimeout;
        this.lockRetryCount = ((OffsetStateManagerSettings) source).lockRetryCount;
        this.source = ((OffsetStateManagerSettings) source).source;
    }
}