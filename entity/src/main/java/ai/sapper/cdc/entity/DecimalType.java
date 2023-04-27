package ai.sapper.cdc.entity;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DecimalType<T> extends NumericType<T> {
    private int scale = 10;
    private int precision = 0;

    public DecimalType() {
    }

    public DecimalType(@NonNull String name,
                       @NonNull Class<? extends T> javaType,
                       int jdbcType) {
        super(name, javaType, jdbcType);
    }

    public DecimalType(@NonNull String name,
                       @NonNull Class<? extends T> javaType,
                       int jdbcType,
                       int scale,
                       int precision) {
        super(name, javaType, jdbcType);
        this.scale = scale;
        this.precision = precision;
    }

    public DecimalType(@NonNull DecimalType<T> source,
                       Integer scale,
                       Integer precision) {
        super(source);
        this.scale = Objects.requireNonNullElseGet(scale, () -> source.scale);
        this.precision = Objects.requireNonNullElseGet(precision, () -> source.precision);
    }

    /**
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (super.equals(o) && o instanceof DecimalType) {
            if (scale > 0 && ((DecimalType<?>) o).scale > 0) {
                if (scale != ((DecimalType<?>) o).scale) return false;
                if (precision > 0 && ((DecimalType<?>) o).precision > 0) {
                    return precision == ((DecimalType<?>) o).precision;
                }
            }
            return true;
        }
        return false;
    }
}
