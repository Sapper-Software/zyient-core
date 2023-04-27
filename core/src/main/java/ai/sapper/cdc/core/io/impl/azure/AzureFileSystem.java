package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.io.impl.CDCFileSystem;
import ai.sapper.cdc.core.io.impl.RemoteFileSystem;
import ai.sapper.cdc.core.keystore.KeyStore;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

public class AzureFileSystem extends RemoteFileSystem {
    private AzureFsClient client;

    /**
     * @param config
     * @param pathPrefix
     * @return
     * @throws IOException
     */
    @Override
    public CDCFileSystem init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              String pathPrefix,
                              KeyStore keyStore) throws IOException {
        try {
            super.init(config, pathPrefix, keyStore);
            client = new AzureFsClient()
                    .init(fsConfig().config(), keyStore);

            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }


    public static AzurePathInfo checkPath(PathInfo pathInfo) throws IOException {
        if (!(pathInfo instanceof AzurePathInfo)) {
            throw new IOException(
                    String.format("Invalid Path type. [type=%s]", pathInfo.getClass().getCanonicalName()));
        }
        return (AzurePathInfo) pathInfo;
    }
}
