package ai.sapper.cdc.entity.types;

import ai.sapper.cdc.common.utils.ReflectionUtils;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.math.BigDecimal;
import java.math.BigInteger;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class NumericType<T> extends DataType<T> {

    public NumericType() {
    }

    public NumericType(@NonNull String name,
                       @NonNull Class<? extends T> javaType,
                       int jdbcType) {
        super(name, javaType, jdbcType);
        Preconditions.checkArgument(ReflectionUtils.isNumericType(javaType)
                || javaType.equals(BigInteger.class)
                || javaType.equals(BigDecimal.class));
    }

    public NumericType(@NonNull DataType<T> source) {
        super(source);
        Preconditions.checkArgument(ReflectionUtils.isNumericType(getJavaType())
                || getJavaType().equals(BigInteger.class)
                || getJavaType().equals(BigDecimal.class));
    }
}
