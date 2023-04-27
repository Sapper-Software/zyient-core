package ai.sapper.cdc.entity.avro;

import ai.sapper.cdc.common.utils.ReflectionUtils;
import ai.sapper.cdc.entity.*;
import ai.sapper.cdc.entity.jdbc.DbEntitySchema;
import ai.sapper.cdc.entity.model.DbDataType;
import ai.sapper.cdc.entity.schema.EntityDiff;
import ai.sapper.cdc.entity.schema.EntitySchema;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Types;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class AvroEntitySchema extends EntitySchema {
    public static final String TYPE_TIME_MILLIS = "time-millis";
    public static final String TYPE_TIME_MICRO = "time-micros";
    public static final String TYPE_TIMESTAMP_MICRO = "timestamp-micros";
    public static final String TYPE_TIMESTAMP_MILLIS = "timestamp-millis";
    public static final DataType<Boolean> BOOLEAN = new DataType<>("boolean", Boolean.class, Types.BOOLEAN);
    public static final DataType<Integer> INTEGER = new IntegerType("int", Integer.MIN_VALUE, Integer.MAX_VALUE, Types.INTEGER);
    public static final DataType<Long> LONG = new NumericType<>("long", Long.class, Types.BIGINT);
    public static final DataType<Float> FLOAT = new DecimalType<>("float", Float.class, Types.FLOAT);
    public static final DataType<Double> DOUBLE = new DecimalType<>("DOUBLE", Double.class, Types.DOUBLE);
    public static final DataType<String> STRING = new TextType("string", Types.VARCHAR, 64 * 1024);
    public static final DataType<String> JSON = new TextType("JSON", -16, 2147483647L);

    public static final DataType<String> ENUM = new TextType("enum", Types.VARCHAR, 64 * 1024);
    public static final DataType<ByteBuffer> BYTES = new BinaryType("bytes", Types.BINARY, 256);
    public static final DataType<GenericRecord> RECORD = new DataType<>("record", GenericRecord.class, Types.STRUCT);
    public static final DataType<Long> TIMESTAMP_MILLIS = new DateTimeType(TYPE_TIMESTAMP_MILLIS, Types.TIMESTAMP);
    public static final DataType<Long> TIMESTAMP_MICROS = new DateTimeType(TYPE_TIMESTAMP_MICRO, Types.TIMESTAMP);
    public static final DataType<Long> TIME_MILLIS = new DateTimeType(TYPE_TIME_MILLIS, Types.TIME);
    public static final DataType<Long> TIME_MICROS = new DateTimeType(TYPE_TIME_MICRO, Types.TIME);
    public static final AvroArray<String> ARRAY = new AvroArray<>();
    public static final AvroMap<String> MAP = new AvroMap<>();

    public AvroEntitySchema() {
        super(RECORD);
    }

    public DataType<?> parseDataType(@NonNull Schema.Field field) {
        Schema schema = null;
        UnionField s = checkUnionType(field);
        if (s == null) schema = field.schema();
        else {
            schema = s.getSchema();
        }
        Schema.Type type = schema.getType();
        DataType<?> dt = getNativeDatatype(schema);
        if (dt != null) return dt;
        if (type == Schema.Type.ARRAY) {
            return new AvroArray<>(field);
        }
        if (type == Schema.Type.MAP) {
            return new AvroMap<>(field);
        }
        if (type == Schema.Type.RECORD) return RECORD;
        if (type == Schema.Type.FIXED) return STRING;
        return null;
    }

    /**
     * @param type
     * @return
     */
    @Override
    public DataType<?> parseDataType(@NonNull Class<?> type) {
        return getDataType(type);
    }

    /**
     * @param column
     * @return
     * @throws Exception
     */
    @Override
    public DataType<?> parseDataType(Metadata.@NonNull ColumnDef column) throws Exception {
        if (BOOLEAN.compare(column.type())) {
            return BOOLEAN;
        } else if (INTEGER.compare(column.type())) {
            return INTEGER;
        } else if (LONG.compare(column.type())) {
            return LONG;
        } else if (FLOAT.compare(column.type())) {
            return FLOAT;
        } else if (DOUBLE.compare(column.type())) {
            return DOUBLE;
        } else if (STRING.compare(column.type())) {
            return STRING;
        } else if (ENUM.compare(column.type())) {
            return ENUM;
        } else if (BYTES.compare(column.type())) {
            return BYTES;
        } else if (RECORD.compare(column.type())) {
            return RECORD;
        }
        throw new Exception(String.format("Data Type not supported. [type=%s]", column.type()));
    }

    /**
     * @return
     * @throws Exception
     */
    @Override
    public Schema generateSchema() throws Exception {
        return generateSchema(false);
    }

    /**
     * @param deep
     * @return
     * @throws Exception
     */
    @Override
    public Schema generateSchema(boolean deep) throws Exception {
        Schema schema = getSchema();
        if (schema == null) {
            Preconditions.checkState(!Strings.isNullOrEmpty(getSchemaStr()));
            load();
        }
        return getSchema();
    }

    /**
     * @param target
     * @return
     * @throws Exception
     */
    @Override
    public EntityDiff diff(@NonNull EntitySchema target) throws Exception {
        if (!(target instanceof AvroEntitySchema)) {
            throw new Exception(
                    String.format("Cannot diff with schema type. [type=%s]",
                            target.getClass().getCanonicalName()));
        }
        return super.diff(target);
    }

    public static DataType<?> getDataType(@NonNull Class<?> type) {
        DataType<?> dt = getNativeDatatype(type);
        if (dt != null) return dt;
        if (type.equals(GenericRecord.class)) return RECORD;
        return null;
    }

    public static DataType<?> getNativeDatatype(@NonNull Class<?> type) {
        if (ReflectionUtils.isBoolean(type)) return BOOLEAN;
        if (ReflectionUtils.isInt(type) || ReflectionUtils.isShort(type)) return INTEGER;
        if (ReflectionUtils.isLong(type)) return LONG;
        if (ReflectionUtils.isFloat(type)) return FLOAT;
        if (ReflectionUtils.isDouble(type)) return DOUBLE;
        if (type.equals(String.class)) return STRING;
        if (type.equals(ByteBuffer.class)) return BYTES;
        return null;
    }

    public static DataType<?> getNativeDatatype(@NonNull Schema schema) {
        if (schema.getLogicalType() != null) {
            LogicalType type = schema.getLogicalType();
            if (type.getName().compareToIgnoreCase(DbEntitySchema.DATE.getName()) == 0) {
                return DbEntitySchema.DATE;
            } else if (type.getName().compareToIgnoreCase(TYPE_TIME_MILLIS) == 0) {
                return TIME_MILLIS;
            } else if (type.getName().compareToIgnoreCase(TYPE_TIME_MICRO) == 0) {
                return TIME_MICROS;
            } else if (type.getName().compareToIgnoreCase(TYPE_TIMESTAMP_MILLIS) == 0) {
                return TIMESTAMP_MILLIS;
            } else if (type.getName().compareToIgnoreCase(TYPE_TIMESTAMP_MICRO) == 0) {
                return TIMESTAMP_MICROS;
            } else if (type.getName().compareToIgnoreCase(DbEntitySchema.DECIMAL.getName()) == 0) {
                LogicalTypes.Decimal d = (LogicalTypes.Decimal) type;
                return DataTypeUtils.createInstance(DbEntitySchema.DECIMAL,
                        d.getScale(),
                        d.getScale(),
                        d.getPrecision());
            }
        }
        Schema.Type type = schema.getType();
        switch (type) {
            case BOOLEAN:
                return BOOLEAN;
            case INT:
                return INTEGER;
            case LONG:
                return LONG;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case ENUM:
                return ENUM;
            case STRING:
                return STRING;
            case BYTES:
                return BYTES;
        }
        return null;
    }

    public static Schema.Type getAvroBasicType(@NonNull DataType<?> dataType) {
        if (dataType.equals(DbEntitySchema.DATE) ||
                dataType.equals(TIME_MILLIS) ||
                dataType.equals(TIME_MICROS) ||
                dataType.equals(TIMESTAMP_MILLIS) ||
                dataType.equals(TIMESTAMP_MICROS)) return Schema.Type.LONG;
        Class<?> type = dataType.getJavaType();
        if (ReflectionUtils.isBoolean(type)) return Schema.Type.BOOLEAN;
        if (ReflectionUtils.isInt(type) || ReflectionUtils.isShort(type)) return Schema.Type.INT;
        if (ReflectionUtils.isLong(type) || type.equals(BigInteger.class)) return Schema.Type.LONG;
        if (ReflectionUtils.isFloat(type)) return Schema.Type.FLOAT;
        if (ReflectionUtils.isDouble(type) || type.equals(BigDecimal.class)) return Schema.Type.DOUBLE;
        if (type.equals(String.class)) return Schema.Type.STRING;
        if (type.equals(ByteBuffer.class)) return Schema.Type.BYTES;

        return null;
    }

    public static boolean isBasicType(@NonNull Schema field) {
        Schema.Type type = field.getType();
        switch (type) {
            case BOOLEAN:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case ENUM:
            case STRING:
                return true;
        }
        return false;
    }

    public static DbDataType.Type getProtoType(@NonNull DataType<?> type) {
        if (type.equals(BOOLEAN)) {
            return DbDataType.Type.BOOLEAN;
        } else if (type.equals(INTEGER)) {
            return DbDataType.Type.INTEGER;
        } else if (type.equals(FLOAT)) {
            return DbDataType.Type.FLOAT;
        } else if (type.equals(DOUBLE)) {
            return DbDataType.Type.DOUBLE;
        } else if (type.isType(DbEntitySchema.DECIMAL)) {
            return DbDataType.Type.DECIMAL;
        } else if (type.isType(STRING)) {
            return DbDataType.Type.VARCHAR;
        } else if (type.isType(BINARY)) {
            return DbDataType.Type.VARBINARY;
        } else if (type.equals(OBJECT)) {
            return DbDataType.Type.RECORD;
        } else if (type.equals(TIMESTAMP_MILLIS) || type.equals(TIMESTAMP_MICROS)) {
            return DbDataType.Type.TIMESTAMP;
        } else if (type.equals(TIME_MILLIS) || type.equals(TIME_MICROS)) {
            return DbDataType.Type.TIME;
        } else if (type.equals(DbEntitySchema.DATE)) {
            return DbDataType.Type.DATE;
        } else if (type.isType(JSON) || type.equals(OBJECT)) {
            return DbDataType.Type.JSON;
        }
        return DbDataType.Type.JSON;
    }
}
