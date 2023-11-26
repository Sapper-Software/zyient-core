/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.cdc.entity.jdbc;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.cdc.entity.Metadata;
import io.zyient.cdc.entity.schema.ArraySchemaField;
import io.zyient.cdc.entity.schema.EntitySchema;
import io.zyient.cdc.entity.schema.MapSchemaField;
import io.zyient.cdc.entity.schema.SchemaField;
import io.zyient.cdc.entity.types.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Types;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class DbEntitySchema extends EntitySchema {
    public static final DataType<Integer> TINYINT = new IntegerType("TINYINT", -128, 127, Types.TINYINT);
    public static final DataType<Integer> SMALLINT = new IntegerType("SMALLINT", Short.MIN_VALUE, Short.MAX_VALUE, Types.SMALLINT);
    public static final DataType<BigInteger> BIGINT = new NumericType<>("BIGINT", BigInteger.class, Types.BIGINT);
    public static final DecimalType<BigDecimal> DECIMAL = new DecimalType<>("DECIMAL", BigDecimal.class, Types.DECIMAL, 0, -1);
    public static final DataType<String> VARCHAR = new TextType("VARCHAR", Types.VARCHAR, 16 * 1024);
    public static final DataType<String> TEXT = new TextType("TEXT", Types.LONGNVARCHAR, 16 * 1024 * 1024);
    public static final DataType<String> LONGTEXT = new TextType("LONGTEXT", Types.LONGNVARCHAR, Integer.MAX_VALUE * 2L);
    public static final DataType<ByteBuffer> VARBINARY = new BinaryType("VARBINARY", Types.VARBINARY, 63 * 1024);
    public static final DataType<ByteBuffer> BLOB = new BinaryType("BLOB", Types.BLOB, 1024 * 1024 * 16);
    public static final DataType<ByteBuffer> LONGBLOB = new BinaryType("LONGBLOB", Types.LONGVARBINARY, Integer.MAX_VALUE * 2L);
    public static final DataType<Long> DATE = new DateTimeType("DATE", Types.DATE);
    public static final DataType<Long> TIME = new DateTimeType("TIME", Types.TIME);
    public static final DataType<Long> DATETIME = new DateTimeType("DATETIME", Types.DATE);
    public static final DataType<Long> TIMESTAMP = new DateTimeType("TIMESTAMP", Types.TIMESTAMP);

    public DbEntitySchema() {
        super(OBJECT);
    }

    public DbEntitySchema(@NonNull EntitySchema source) {
        super(source);
        setRecordType(OBJECT);
    }

    /**
     * @param field
     * @return
     */
    @Override
    public DataType<?> parseDataType(Schema.@NonNull Field field) {
        Schema schema = null;
        UnionField s = checkUnionType(field);
        if (s == null) schema = field.schema();
        else {
            schema = s.getSchema();
        }
        Schema.Type type = schema.getType();
        if (type == Schema.Type.BOOLEAN) return BOOLEAN;
        if (type == Schema.Type.INT) return INTEGER;
        if (type == Schema.Type.LONG) return BIGINT;
        if (type == Schema.Type.FLOAT) return FLOAT;
        if (type == Schema.Type.DOUBLE) return DOUBLE;
        if (type == Schema.Type.ENUM) return VARCHAR;
        if (type == Schema.Type.STRING) return VARCHAR;
        if (type == Schema.Type.BYTES) return VARBINARY;
        if (type == Schema.Type.ARRAY) return ArraySchemaField.ARRAY;
        if (type == Schema.Type.MAP) return MapSchemaField.MAP;
        if (type == Schema.Type.RECORD) return OBJECT;
        if (type == Schema.Type.FIXED) return VARCHAR;
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
    public DataType<?> parseDataType(@NonNull Metadata.ColumnDef column) throws Exception {
        DataType<?> dt = checkNumericTypes(column.type());
        if (dt != null) return dt;
        dt = checkStringTypes(column.type());
        if (dt != null) return dt;
        dt = checkBinaryTypes(column.type());
        if (dt != null) return dt;
        dt = checkDateTypes(column.type());
        return dt;
    }

    public DataType<?> checkNumericTypes(@NonNull String type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(type));
        if (BOOLEAN.compare(type)
                || type.compareToIgnoreCase("BOOL") == 0) {
            return BOOLEAN;
        } else if (TINYINT.compare(type)) {
            return TINYINT;
        } else if (SMALLINT.compare(type)) {
            return SMALLINT;
        } else if (INTEGER.compare(type)
                || type.compareToIgnoreCase("INT") == 0) {
            return INTEGER;
        } else if (BIGINT.compare(type)) {
            return BIGINT;
        } else if (FLOAT.compare(type)) {
            return FLOAT;
        } else if (DOUBLE.compare(type)) {
            return DOUBLE;
        } else if (DECIMAL.compare(type)
                || type.compareToIgnoreCase("NUMERIC") == 0) {
            return DECIMAL;
        }
        return null;
    }

    public DataType<?> checkStringTypes(@NonNull String type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(type));
        if (CHAR.compare(type)) {
            return CHAR;
        } else if (VARCHAR.compare(type)) {
            return VARCHAR;
        } else if (TEXT.compare(type)) {
            return TEXT;
        } else if (LONGTEXT.compare(type)) {
            return LONGTEXT;
        }
        return null;
    }

    public DataType<?> checkBinaryTypes(@NonNull String type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(type));
        if (BINARY.compare(type)) {
            return BINARY;
        } else if (VARBINARY.compare(type)) {
            return VARBINARY;
        } else if (LONGBLOB.compare(type)) {
            return LONGBLOB;
        } else if (BLOB.compare(type)) {
            return BLOB;
        }
        return null;
    }

    public DataType<?> checkDateTypes(@NonNull String type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(type));
        if (DATE.compare(type)) {
            return DATE;
        } else if (TIME.compare(type)) {
            return TIME;
        } else if (DATETIME.compare(type)) {
            return DATETIME;
        } else if (TIMESTAMP.compare(type)) {
            return TIMESTAMP;
        }
        return null;
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
        Preconditions.checkNotNull(getFields());
        Preconditions.checkState(!getFields().isEmpty());
        if (deep && getRecordTypes() != null && getRecordTypes().size() > 0) {
            return generateComplexSchema(getMapper());
        }
        SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(getName());
        builder.namespace(getNamespace());
        SchemaBuilder.FieldAssembler<Schema> fields = builder.fields();
        for (String name : getFields().keySet()) {
            SchemaField field = get(name);
            fields = addSchemaField(field, fields, getMapper(), deep);
        }

        withSchema(fields.endRecord(), false);
        return getSchema();
    }



    public static DataType<?> getDataType(@NonNull Class<?> type) {
        DataType<?> dt = getNativeDatatype(type);
        if (dt != null) return dt;
        if (type.equals(GenericRecord.class)) return OBJECT;
        return null;
    }

    public static DataType<?> getNativeDatatype(@NonNull Class<?> type) {
        if (ReflectionHelper.isBoolean(type)) return BOOLEAN;
        if (ReflectionHelper.isShort(type)) return SMALLINT;
        if (ReflectionHelper.isInt(type)) return INTEGER;
        if (ReflectionHelper.isLong(type)) return BIGINT;
        if (ReflectionHelper.isFloat(type)) return FLOAT;
        if (ReflectionHelper.isDouble(type)) return DOUBLE;
        if (type.equals(String.class)) return new TextType(VARCHAR.getName(), Types.VARCHAR,
                ((SizedDataType<?>) VARCHAR).getSize());
        if (type.equals(ByteBuffer.class) ||
                type.equals(byte.class)) return new BinaryType(VARBINARY.getName(), Types.VARBINARY,
                ((SizedDataType<?>) VARBINARY).getSize());
        return null;
    }

    public static int getJdbcDatatype(@NonNull DataType<?> type) {
        if (type.equals(BOOLEAN)) {
            return Types.BOOLEAN;
        } else if (type.equals(TINYINT)) {
            return Types.TINYINT;
        } else if (type.equals(SMALLINT)) {
            return Types.SMALLINT;
        } else if (type.equals(INTEGER)) {
            return Types.INTEGER;
        } else if (type.equals(BIGINT)) {
            return Types.BIGINT;
        } else if (type.equals(FLOAT)) {
            return Types.FLOAT;
        } else if (type.equals(DOUBLE)) {
            return Types.DOUBLE;
        } else if (type.equals(DECIMAL)) {
            return Types.DECIMAL;
        } else if (type.equals(VARCHAR)) {
            return Types.VARCHAR;
        } else if (type.equals(LONGTEXT)) {
            return Types.LONGVARCHAR;
        } else if (type.equals(VARBINARY)) {
            return Types.VARBINARY;
        } else if (type.equals(BLOB)) {
            return Types.BLOB;
        } else if (type.equals(LONGBLOB)) {
            return Types.LONGVARBINARY;
        } else if (type.equals(DATE)) {
            return Types.DATE;
        } else if (type.equals(TIME)) {
            return Types.TIME;
        } else if (type.equals(TIMESTAMP)) {
            return Types.TIMESTAMP;
        }
        return Types.VARCHAR;
    }
}
