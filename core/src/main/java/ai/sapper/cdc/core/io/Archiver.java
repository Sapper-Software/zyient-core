package ai.sapper.cdc.core.io;

import ai.sapper.cdc.core.io.model.ArchivePathInfo;
import ai.sapper.cdc.core.io.model.PathInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class Archiver implements Closeable {
    public static final String CONFIG_ARCHIVER = "archiver";

    public abstract void init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                              String pathPrefix) throws IOException;

    public abstract ArchivePathInfo archive(@NonNull PathInfo source,
                                            @NonNull ArchivePathInfo target,
                                            @NonNull FileSystem sourceFS) throws IOException;

    public abstract File getFromArchive(@NonNull ArchivePathInfo path) throws IOException;
}
