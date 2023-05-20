package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.core.io.impl.RemoteReader;
import ai.sapper.cdc.core.io.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class AzureReader extends RemoteReader {
    private final AzurePathInfo pathInfo;

    public AzureReader(@NonNull AzureFileSystem fs,
                       @NonNull FileInode path) throws IOException {
        super(path, fs);
        if (path.getPathInfo() != null) {
            pathInfo = (AzurePathInfo) path.getPathInfo();
        } else {
            pathInfo = (AzurePathInfo) fs.parsePathInfo(path.getPath());
            path.setPathInfo(pathInfo);
        }
    }
}
