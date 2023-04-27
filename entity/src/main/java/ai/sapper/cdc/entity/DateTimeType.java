package ai.sapper.cdc.entity;

import lombok.NonNull;

public class DateTimeType extends DataType<Long> {
    public DateTimeType() {
    }

    public DateTimeType(@NonNull String name, int jdbcType) {
        super(name, Long.class, jdbcType);
    }

    public DateTimeType(@NonNull DataType<Long> source) {
        super(source);
    }
}
