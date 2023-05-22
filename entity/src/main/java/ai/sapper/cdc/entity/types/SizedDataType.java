package ai.sapper.cdc.entity.types;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class SizedDataType<T> extends DataType<T> {
    private long size = -1;

    public SizedDataType() {
    }

    public SizedDataType(@NonNull String name,
                         @NonNull Class<? extends T> type,
                         int jdbcType,
                         long size) {
        super(name, type, jdbcType);
        if (size > 0) this.size = size;
    }

    public SizedDataType(@NonNull SizedDataType<T> source,
                         long size) {
        super(source);
        Preconditions.checkArgument(size > 0);
        this.size = size;
    }

    /**
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (super.equals(o) && o instanceof SizedDataType) {
            if (size > 0 && ((SizedDataType<?>) o).size > 0) {
                return size == ((SizedDataType<?>) o).size;
            }
            return true;
        }
        return false;
    }
}
