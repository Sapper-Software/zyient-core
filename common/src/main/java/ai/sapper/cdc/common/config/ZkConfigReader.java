package ai.sapper.cdc.common.config;

import ai.sapper.cdc.common.utils.JSONUtils;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.curator.framework.CuratorFramework;

@Getter
@Accessors(fluent = true)
public final class ZkConfigReader {
    private final CuratorFramework client;
    private final Class<? extends Settings> type;
    private Settings settings;

    public ZkConfigReader(@NonNull CuratorFramework client,
                          @NonNull Class<? extends Settings> type) {
        this.client = client;
        this.type = type;
    }

    public void save(@NonNull String path) throws ConfigurationException {
        Preconditions.checkNotNull(settings);
        try {
            if (client.checkExists().forPath(path) != null) {
                client.create().creatingParentContainersIfNeeded().forPath(path);
            }
            byte[] data = JSONUtils.asBytes(settings, settings.getClass());
            client.setData().forPath(path, data);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public boolean read(@NonNull String path) throws ConfigurationException {
        try {
            if (client.checkExists().forPath(path) != null) {
                byte[] data = client.getData().forPath(path);
                if (data != null && data.length > 0) {
                    settings = JSONUtils.read(data, settings.getClass());
                }
            }
            return false;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }
}
