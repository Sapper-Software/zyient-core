package ai.sapper.cdc.entity.types;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.Objects;

@Getter
@Setter
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class DataType<T> {
    private String name;
    private Class<? extends T> javaType;
    private int jdbcType;

    public DataType() {
    }

    public DataType(@NonNull String name,
                    @NonNull Class<? extends T> javaType,
                    int jdbcType) {
        this.name = name;
        this.javaType = javaType;
        this.jdbcType = jdbcType;
    }

    public DataType(@NonNull DataType<T> source) {
        this.name = source.name;
        this.javaType = source.javaType;
        this.jdbcType = source.jdbcType;
    }

    public boolean compare(@NonNull String name) {
        return this.name.compareToIgnoreCase(name) == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataType<?> dataType = (DataType<?>) o;
        return name.equals(dataType.name) && javaType.equals(dataType.javaType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, javaType);
    }

    public boolean isType(@NonNull DataType<?> type) {
        return (name.equals(type.name) && javaType.equals(type.javaType));
    }
}
