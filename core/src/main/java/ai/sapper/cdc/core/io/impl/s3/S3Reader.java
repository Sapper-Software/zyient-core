package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.core.io.impl.RemoteReader;
import ai.sapper.cdc.core.io.model.FileInode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class S3Reader extends RemoteReader {
    private final S3PathInfo pathInfo;

    public S3Reader(@NonNull S3FileSystem fs,
                    @NonNull FileInode path) throws IOException {
        super(path, fs);
        if (path.getPathInfo() != null) {
            pathInfo = (S3PathInfo) path.getPathInfo();
        } else {
            pathInfo = (S3PathInfo) fs.parsePathInfo(path.getPath());
            path.setPathInfo(pathInfo);
        }
    }
}
