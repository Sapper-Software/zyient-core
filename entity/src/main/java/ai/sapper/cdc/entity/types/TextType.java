package ai.sapper.cdc.entity.types;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class TextType extends SizedDataType<String> {
    private static final long DEFAULT_SIZE = 1024 * 64;

    public TextType() {

    }

    public TextType(@NonNull String name, int jdbcType, long size) {
        super(name, String.class, jdbcType, size);
        if (getSize() <= 0) setSize(DEFAULT_SIZE);
    }

    public TextType(@NonNull TextType source, long size) {
        super(source, size);
    }

    public String checkSize(@NonNull String data) {
        if (data.length() > getSize()) {
            data = data.substring(0, (int) getSize());
        }
        return data;
    }
}