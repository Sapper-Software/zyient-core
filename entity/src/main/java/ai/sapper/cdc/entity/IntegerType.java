package ai.sapper.cdc.entity;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class IntegerType extends NumericType<Integer> {
    private int rangeMin;
    private int rangeMax;

    public IntegerType() {
    }

    public IntegerType(@NonNull String name, int rangeMin, int rangeMax, int jdbcType) {
        super(name, Integer.class, jdbcType);
        Preconditions.checkArgument(rangeMin < rangeMax);
        this.rangeMin = rangeMin;
        this.rangeMax = rangeMax;
    }

    public IntegerType(@NonNull IntegerType source, Integer rangeMin, Integer rangeMax) {
        super(source);
        this.rangeMin = Objects.requireNonNullElseGet(rangeMin, () -> source.rangeMin);
        this.rangeMax = Objects.requireNonNullElseGet(rangeMax, () -> source.rangeMax);
    }

    public IntegerType(@NonNull IntegerType source) {
        super(source);
        this.rangeMax = source.rangeMax;
        this.rangeMin = source.rangeMin;
    }

    public int getInRange(int value) {
        if (value < rangeMin) value = rangeMin;
        else if (value > rangeMax) value = rangeMax;
        return value;
    }
}
