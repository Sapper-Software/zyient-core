package ai.sapper.cdc.core.io.impl.s3;

import ai.sapper.cdc.core.io.impl.RemoteReader;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.io.model.PathInfo;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.impl.local.LocalReader;
import com.google.common.base.Preconditions;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

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
