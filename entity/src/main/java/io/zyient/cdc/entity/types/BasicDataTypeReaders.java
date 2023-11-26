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

package io.zyient.cdc.entity.types;

import com.google.common.base.Strings;
import io.zyient.base.common.utils.ReflectionHelper;
import io.zyient.cdc.entity.utils.ConversionUtils;
import io.zyient.cdc.entity.utils.DateTimeHelper;
import lombok.NonNull;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.UUID;

public class BasicDataTypeReaders {
    public static BooleanReader BOOLEAN_READER = new BooleanReader();
    public static IntegerReader INTEGER_READER = new IntegerReader();
    public static LongReader LONG_READER = new LongReader();
    public static FloatReader FLOAT_READER = new FloatReader();
    public static DoubleReader DOUBLE_READER = new DoubleReader();
    public static StringReader STRING_READER = new StringReader();
    public static BytesReader BYTES_READER = new BytesReader();

    public static DataTypeReader<?> getReader(@NonNull DataType<?> type) throws Exception {
        if (ReflectionHelper.isBoolean(type.getJavaType())) {
            return BOOLEAN_READER;
        } else if (ReflectionHelper.isInt(type.getJavaType())) {
            return INTEGER_READER;
        } else if (ReflectionHelper.isLong(type.getJavaType())) {
            return LONG_READER;
        } else if (ReflectionHelper.isFloat(type.getJavaType())) {
            return FLOAT_READER;
        } else if (ReflectionHelper.isDouble(type.getJavaType())) {
            return DOUBLE_READER;
        } else if (type.getJavaType().equals(String.class)) {
            return STRING_READER;
        } else if (type.getJavaType().equals(ByteBuffer.class)) {
            return BYTES_READER;
        }
        return null;
    }

    public static class BooleanReader implements DataTypeReader<Boolean> {

        /**
         * @param data
         * @return
         * @throws Exception
         */
        @Override
        public Boolean read(@NonNull Object data) throws Exception {
            if (data instanceof Boolean) {
                return (Boolean) data;
            } else if (data instanceof String) {
                if (!Strings.isNullOrEmpty((String) data)) {
                    return Boolean.parseBoolean((String) data);
                }
            } else if (data instanceof Utf8) {
                String s = data.toString();
                if (!Strings.isNullOrEmpty(s)) {
                    return Boolean.parseBoolean(data.toString());
                }
            } else if (ReflectionHelper.isNumericType(data.getClass())) {
                Long value = ConversionUtils.getLong(data);
                if (value == null) {
                    throw new Exception("DataType conversion returned null.");
                }
                if (value == 0) {
                    return false;
                } else if (value == 1) {
                    return true;
                }
            }
            return null;
        }

        /**
         * @return
         */
        @Override
        public Class<? extends Boolean> getType() {
            return Boolean.class;
        }
    }

    public static class IntegerReader implements DataTypeReader<Integer> {

        /**
         * @param data
         * @return
         * @throws Exception
         */
        @Override
        public Integer read(@NonNull Object data) throws Exception {
            if (data instanceof String) {
                if (!Strings.isNullOrEmpty((String) data)) {
                    return Integer.parseInt((String) data);
                }
            } else if (data instanceof Utf8) {
                String s = data.toString();
                if (!Strings.isNullOrEmpty(s)) {
                    return Integer.parseInt(data.toString());
                }
            } else if (ReflectionHelper.isNumericType(data.getClass())) {
                return ConversionUtils.getInt(data);
            } else if (data instanceof BigInteger) {
                BigInteger bi = (BigInteger) data;
                return bi.intValue();
            } else if (data instanceof BigDecimal) {
                BigDecimal bd = (BigDecimal) data;
                return bd.intValue();
            } else if (data instanceof Boolean) {
                Boolean b = (Boolean) data;
                return (b ? 1 : 0);
            }
            return null;
        }

        /**
         * @return
         */
        @Override
        public Class<? extends Integer> getType() {
            return Integer.class;
        }
    }

    public static class LongReader implements DataTypeReader<Long> {

        /**
         * @param data
         * @return
         * @throws Exception
         */
        @Override
        public Long read(@NonNull Object data) throws Exception {
            if (data instanceof String) {
                if (!Strings.isNullOrEmpty((String) data)) {
                    return Long.parseLong((String) data);
                }
            } else if (data instanceof Utf8) {
                String s = data.toString();
                if (!Strings.isNullOrEmpty(s)) {
                    return Long.parseLong(data.toString());
                }
            } else if (ReflectionHelper.isNumericType(data.getClass())) {
                return ConversionUtils.getLong(data);
            } else if (data instanceof BigInteger) {
                BigInteger bi = (BigInteger) data;
                return bi.longValue();
            } else if (data instanceof BigDecimal) {
                BigDecimal bd = (BigDecimal) data;
                return bd.longValue();
            }
            return null;
        }

        /**
         * @return
         */
        @Override
        public Class<? extends Long> getType() {
            return Long.class;
        }
    }

    public static class FloatReader implements DataTypeReader<Float> {

        /**
         * @param data
         * @return
         * @throws Exception
         */
        @Override
        public Float read(@NonNull Object data) throws Exception {
            if (data instanceof String) {
                if (!Strings.isNullOrEmpty((String) data)) {
                    return Float.parseFloat((String) data);
                }
            } else if (data instanceof Utf8) {
                String s = data.toString();
                if (!Strings.isNullOrEmpty(s)) {
                    return Float.parseFloat(data.toString());
                }
            } else if (ReflectionHelper.isNumericType(data.getClass())) {
                return ConversionUtils.getFloat(data);
            } else if (data instanceof BigInteger) {
                BigInteger bi = (BigInteger) data;
                return bi.floatValue();
            } else if (data instanceof BigDecimal) {
                BigDecimal bd = (BigDecimal) data;
                return bd.floatValue();
            }
            return null;
        }

        /**
         * @return
         */
        @Override
        public Class<? extends Float> getType() {
            return Float.class;
        }
    }

    public static class DoubleReader implements DataTypeReader<Double> {

        /**
         * @param data
         * @return
         * @throws Exception
         */
        @Override
        public Double read(@NonNull Object data) throws Exception {
            if (data instanceof String) {
                if (!Strings.isNullOrEmpty((String) data)) {
                    return Double.parseDouble((String) data);
                }
            } else if (data instanceof Utf8) {
                String s = data.toString();
                if (!Strings.isNullOrEmpty(s)) {
                    return Double.parseDouble(data.toString());
                }
            } else if (ReflectionHelper.isNumericType(data.getClass())) {
                return ConversionUtils.getDouble(data);
            } else if (data instanceof BigInteger) {
                BigInteger bi = (BigInteger) data;
                return bi.doubleValue();
            } else if (data instanceof BigDecimal) {
                BigDecimal bd = (BigDecimal) data;
                return bd.doubleValue();
            }
            return null;
        }

        /**
         * @return
         */
        @Override
        public Class<? extends Double> getType() {
            return Double.class;
        }
    }

    public static class StringReader implements DataTypeReader<String> {

        /**
         * @param data
         * @return
         * @throws Exception
         */
        @Override
        public String read(@NonNull Object data) throws Exception {
            if (data instanceof String) {
                return (String) data;
            } else if (data instanceof Utf8) {
                return data.toString();
            } else if (ReflectionHelper.isNumericType(data.getClass())) {
                return String.valueOf(data);
            } else if (data instanceof byte[]) {
                byte[] b = (byte[]) data;
                return new String(b, Charset.defaultCharset());
            } else if (data instanceof ByteBuffer) {
                ByteBuffer bb = (ByteBuffer) data;
                bb.rewind();
                return new String(bb.array(), Charset.defaultCharset());
            } else if (data instanceof UUID) {
                return data.toString();
            } else if (data instanceof Date) {
                return DateTimeHelper.toISODateString((Date) data);
            }
            throw new Exception(
                    String.format("Cannot convert to String: [type=%s]",
                            data.getClass().getCanonicalName()));
        }

        /**
         * @return
         */
        @Override
        public Class<? extends String> getType() {
            return String.class;
        }
    }

    public static class BytesReader implements DataTypeReader<ByteBuffer> {

        /**
         * @param data
         * @return
         * @throws Exception
         */
        @Override
        public ByteBuffer read(@NonNull Object data) throws Exception {
            if (data instanceof byte[]) {
                byte[] bb = (byte[]) data;
                return ByteBuffer.wrap(bb).rewind();
            } else if (data instanceof String) {
                if (!Strings.isNullOrEmpty((String) data)) {
                    byte[] bb = ((String) data).getBytes(Charset.defaultCharset());
                    return ByteBuffer.wrap(bb).rewind();
                }
            } else if (data instanceof Utf8) {
                String s = data.toString();
                if (!Strings.isNullOrEmpty(s)) {
                    byte[] bb = data.toString().getBytes(Charset.defaultCharset());
                    return ByteBuffer.wrap(bb).rewind();
                }
            } else if (data instanceof ByteBuffer) {
                return (ByteBuffer) data;
            }
            return null;
        }

        /**
         * @return
         */
        @Override
        public Class<? extends ByteBuffer> getType() {
            return ByteBuffer.class;
        }
    }
}
