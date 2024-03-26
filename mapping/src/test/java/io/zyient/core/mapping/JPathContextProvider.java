package io.zyient.core.mapping;

import com.google.common.base.Preconditions;
import io.zyient.base.core.utils.FileTypeDetector;
import io.zyient.base.core.utils.SourceTypes;
import io.zyient.core.mapping.model.ContentInfo;
import io.zyient.core.mapping.model.InputContentInfo;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;

public class JPathContextProvider extends DemoContextProvider{
    @Override
    public InputContentInfo inputContext(@NonNull ContentInfo contentInfo) throws Exception {
        Preconditions.checkArgument(contentInfo instanceof InputContentInfo);
        InputContentInfo info = (InputContentInfo) contentInfo;
        File file = info.path();
        if (file == null) {
            throw new Exception("Input file not specified...");
        }
        if (!file.exists()) {
            throw new IOException(String.format("Input file not found. [path=%s]", file.getAbsolutePath()));
        }
        SourceTypes st = info.contentType();
        if (st == null) {
            FileTypeDetector detector = new FileTypeDetector(file);
            detector.detect();
            st = detector.type();
            if (st == null) {
                throw new Exception(String.format("Failed to detect content type. [file=%s]", file.getAbsolutePath()));
            }
            info.contentType(st);
        }
        info.mapping("test-customer");
        return info;
    }
}
