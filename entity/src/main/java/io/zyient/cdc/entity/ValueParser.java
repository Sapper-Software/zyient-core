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

package io.zyient.cdc.entity;

import com.google.protobuf.ByteString;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.cdc.entity.schema.EntitySchema;
import io.zyient.cdc.entity.types.*;
import lombok.NonNull;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

public abstract class ValueParser {
    public static final String TYPE_REGEX = "(.+)\\s+(.+)$";
    public static final Pattern TYPE_PATTERN = Pattern.compile(TYPE_REGEX);

    public long dateToDateTime(long value) {
        return value * 24 * 60 * 60 * 1000;
    }

    public long microToMilliSeconds(long value) {
        return value / 1000;
    }

    public boolean updatePrimitiveValue(@NonNull DbPrimitiveValue.Builder vb,
                                        @NonNull DataType<?> type,
                                        Object value) throws Exception {
        if (value == null) {
            vb.setNull(NullColumnData.newBuilder().setNull(true).build());
            return true;
        }
        if (type instanceof DateTimeType) {
            return false;
        }
        if (type.equals(EntitySchema.OBJECT)
                || type.equals(EntitySchema.JSON)) {
            String json = JSONUtils.asString(value, value.getClass());
            byte[] bytes = json.getBytes(Charset.defaultCharset());
            JsonColumnData data = JsonColumnData.newBuilder()
                    .setData(ByteString.copyFrom(bytes))
                    .setType(value.getClass().getCanonicalName())
                    .build();
            vb.setJson(data);
            return true;
        }
        if (ReflectionHelper.isBoolean(type.getJavaType())) {
            boolean v = BasicDataTypeReaders.BOOLEAN_READER.read(value);
            BooleanColumnData data = BooleanColumnData.newBuilder()
                    .setData(v)
                    .build();
            vb.setBoolean(data);
            return true;
        }
        if (ReflectionHelper.isInt(type.getJavaType()) ||
                ReflectionHelper.isShort(type.getJavaType())) {
            int v = BasicDataTypeReaders.INTEGER_READER.read(value);
            IntegerColumnData data = IntegerColumnData.newBuilder()
                    .setData(v)
                    .build();
            vb.setInteger(data);
            return true;
        }
        if (ReflectionHelper.isLong(type.getJavaType())) {
            long v = BasicDataTypeReaders.LONG_READER.read(value);
            LongColumnData data = LongColumnData.newBuilder()
                    .setData(v)
                    .build();
            vb.setLong(data);
            return true;
        }
        if (ReflectionHelper.isFloat(type.getJavaType())) {
            float v = BasicDataTypeReaders.FLOAT_READER.read(value);
            FloatColumnData data = FloatColumnData.newBuilder()
                    .setData(v)
                    .build();
            vb.setFloat(data);
            return true;
        }
        if (ReflectionHelper.isDouble(type.getJavaType())) {
            double v = BasicDataTypeReaders.DOUBLE_READER.read(value);
            DoubleColumnData data = DoubleColumnData.newBuilder()
                    .setData(v)
                    .build();
            vb.setDouble(data);
            return true;
        }
        if (type instanceof BinaryType) {
            ByteBuffer bb = BasicDataTypeReaders.BYTES_READER.read(value);
            BinaryColumnData data = BinaryColumnData.newBuilder()
                    .setData(ByteString.copyFrom(bb.array()))
                    .build();
            vb.setBytes(data);
            return true;
        }
        if (type instanceof TextType) {
            String ss = BasicDataTypeReaders.STRING_READER.read(value);
            String charset = Charset.defaultCharset().name();
            byte[] bytes = ss.getBytes(Charset.defaultCharset());
            StringColumnData data = StringColumnData.newBuilder()
                    .setData(ByteString.copyFrom(bytes))
                    .setEncoding(charset)
                    .build();
            vb.setString(data);
            return true;
        }
        return false;
    }

    public abstract DataType<?> parseDataType(@NonNull String typeName,
                                              int jdbcType,
                                              long size,
                                              int... params) throws Exception;
}
