package ai.sapper.cdc.core.io.utils;

import ai.sapper.cdc.core.io.Writer;
import com.google.protobuf.GeneratedMessageV3;
import lombok.NonNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public class ProtobufWriter<T extends GeneratedMessageV3> implements Closeable {
    private final OutputStream outputStream;
    private final Writer writer;

    public ProtobufWriter(@NonNull Writer writer) throws Exception {
        this.outputStream = writer.getOutputStream();
        this.writer = writer;
    }

    public void write(@NonNull T data) throws IOException {
        data.writeDelimitedTo(outputStream);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
