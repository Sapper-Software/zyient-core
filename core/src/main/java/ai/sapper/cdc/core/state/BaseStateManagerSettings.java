package ai.sapper.cdc.core.state;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.common.config.Settings;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class BaseStateManagerSettings extends Settings  {
    public static final String __CONFIG_PATH = "state";
    public static final String __CONFIG_PATH_OFFSET_MANAGERS = "offsets";

    public static final class Constants {
        public static final short LOCK_RETRY_COUNT = 4;
        public static final long LOCK_TIMEOUT = 15000;

        public static final String CONFIG_ZK_BASE = "basePath";
        public static final String CONFIG_ZK_CONNECTION = "connection";
        public static final String CONFIG_LOCK_RETRY = "locking.retry";
        public static final String CONFIG_LOCK_TIMEOUT = "locking.timeout";
        public static final String CONFIG_SAVE_OFFSETS = String.format("%s.save", __CONFIG_PATH_OFFSET_MANAGERS);
    }

    @Config(name = Constants.CONFIG_ZK_BASE)
    private String basePath;
    @Config(name = Constants.CONFIG_ZK_CONNECTION)
    private String zkConnection;
    @Config(name = Constants.CONFIG_LOCK_RETRY, required = false, type = Short.class)
    private short lockRetryCount = Constants.LOCK_RETRY_COUNT;
    @Config(name = Constants.CONFIG_LOCK_TIMEOUT, required = false, type = Long.class)
    private long lockTimeout = Constants.LOCK_TIMEOUT;
    private boolean saveOffsetManager = true;

    public BaseStateManagerSettings() {
    }

    public BaseStateManagerSettings(@NonNull Settings source) {
        super(source);
        Preconditions.checkArgument(source instanceof BaseStateManagerSettings);
        this.basePath = ((BaseStateManagerSettings) source).basePath;
        this.zkConnection = ((BaseStateManagerSettings) source).zkConnection;
        this.lockTimeout = ((BaseStateManagerSettings) source).lockTimeout;
        this.lockRetryCount = ((BaseStateManagerSettings) source).lockRetryCount;
        this.saveOffsetManager = ((BaseStateManagerSettings) source).saveOffsetManager;
    }
}
