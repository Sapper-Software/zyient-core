package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.core.io.impl.RemoteWriter;
import ai.sapper.cdc.core.io.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class AzureWriter extends RemoteWriter {
    private final AzurePathInfo pathInfo;

    protected AzureWriter(@NonNull FileInode path,
                          @NonNull AzureFileSystem fs,
                          boolean overwrite) throws IOException {
        super(path, fs, overwrite);
        if (path.getPathInfo() != null) {
            pathInfo = (AzurePathInfo) path.getPathInfo();
        } else {
            pathInfo = (AzurePathInfo) fs.parsePathInfo(path.getPath());
            path.setPathInfo(pathInfo);
        }
    }

    @Override
    protected String getTmpPath() {
        String dir = FilenameUtils.getPath(pathInfo.path());
        return String.format("%s/%s", pathInfo.container(), dir);
    }
}
