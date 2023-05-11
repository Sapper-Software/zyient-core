package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.core.io.impl.RemoteWriter;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.impl.local.LocalWriter;
import lombok.NonNull;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;

public class S3Writer extends RemoteWriter {
    private final S3PathInfo pathInfo;

    protected S3Writer(@NonNull FileInode path,
                       @NonNull S3FileSystem fs,
                       boolean overwrite) throws IOException {
        super(path, fs, overwrite);
        if (path.getPathInfo() != null) {
            pathInfo = (S3PathInfo) path.getPathInfo();
        } else {
            pathInfo = (S3PathInfo) fs.parsePathInfo(path.getPath());
            path.setPathInfo(pathInfo);
        }
    }

    @Override
    protected String getTmpPath() {
        String dir = FilenameUtils.getPath(pathInfo.path());
        return String.format("%s/%s", pathInfo.bucket(), dir);
    }
}
