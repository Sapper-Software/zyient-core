package ai.sapper.cdc.entity.types;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.Base64;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class BinaryType extends SizedDataType<ByteBuffer> {
    private static final long DEFAULT_SIZE = 1024 * 1024 * 1024;

    public BinaryType() {
    }

    public BinaryType(@NonNull String name, int jdbcType, long size) {
        super(name, ByteBuffer.class, jdbcType, size);
        if (this.getSize() < 0) setSize(DEFAULT_SIZE);
    }

    public BinaryType(@NonNull BinaryType source, long size) {
        super(source, size);
    }

    public static String convert(@NonNull ByteBuffer data, long size) throws Exception {
        String s = Base64.getEncoder().encodeToString(data.array());
        if (s.length() > size) {
            throw new Exception(String.format("Exceeded size limit for field. [limit=%d]size=%d]", size, s.length()));
        }
        return s;
    }
}
