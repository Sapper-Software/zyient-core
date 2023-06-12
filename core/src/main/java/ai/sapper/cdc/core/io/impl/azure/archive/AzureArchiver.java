package ai.sapper.cdc.core.io.impl.azure.archive;

import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.impl.azure.AzureFsClient;
import ai.sapper.cdc.core.io.model.ArchivePathInfo;
import ai.sapper.cdc.core.io.model.ArchiverSettings;
import ai.sapper.cdc.core.io.model.PathInfo;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.IOException;

public class AzureArchiver extends Archiver {
    private AzureFsClient client;

    @Override
    public Archiver init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                         String pathPrefix) throws IOException {
        try {
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public Archiver init(@NonNull ArchiverSettings settings) throws IOException {
        try {
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public ArchivePathInfo archive(@NonNull PathInfo source,
                                   @NonNull ArchivePathInfo target,
                                   @NonNull FileSystem sourceFS) throws IOException {
        return null;
    }

    @Override
    public File getFromArchive(@NonNull String domain,
                               @NonNull String path) throws IOException {
        return null;
    }


    @Override
    public void close() throws IOException {

    }
}
