package ai.sapper.cdc.entity.avro;

import ai.sapper.cdc.entity.types.BasicDataTypeReaders;
import ai.sapper.cdc.entity.types.DataType;
import ai.sapper.cdc.entity.types.DataTypeReader;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

public class AvroDataTypeReaders {

    public static RecordReader RECORD_READER = new RecordReader();

    public static DataTypeReader<?> getReader(@NonNull DataType<?> type) throws Exception {
        DataTypeReader<?> reader = BasicDataTypeReaders.getReader(type);
        if (reader == null) {
            if (type.getJavaType().equals(GenericRecord.class)) {
                return RECORD_READER;
            } else if (type instanceof AvroArray) {
                AvroArray<?> at = (AvroArray<?>) type;
                return new ArrayReader(at);
            } else if (type instanceof AvroMap) {
                AvroMap<?> am = (AvroMap<?>) type;
                return new MapReader(am);
            }
            return null;
        } else
            return reader;
    }

    public static class RecordReader implements DataTypeReader<GenericRecord> {

        /**
         * @param data
         * @return
         * @throws Exception
         */
        @Override
        public GenericRecord read(@NonNull Object data) throws Exception {
            if (data instanceof GenericRecord) {
                return (GenericRecord) data;
            }
            return null;
        }

        /**
         * @return
         */
        @Override
        public Class<? extends GenericRecord> getType() {
            return GenericRecord.class;
        }
    }

    public static class ArrayReader implements DataTypeReader<List<?>> {
        private Class<?> inner;
        private DataType<?> innerType;

        public ArrayReader(@NonNull AvroArray<?> type) {
            this.inner = type.getInner();
            this.innerType = AvroEntitySchema.getDataType(this.inner);
            Preconditions.checkNotNull(this.innerType);
        }

        /**
         * @param data
         * @return
         * @throws Exception
         */
        @Override
        public List<?> read(@NonNull Object data) throws Exception {
            Preconditions.checkNotNull(inner);
            Preconditions.checkArgument(data instanceof List);
            return (List<?>) data;
        }

        /**
         * @return
         */
        @Override
        public Class<?> getType() {
            return List.class;
        }
    }

    public static class MapReader implements DataTypeReader<Map<String, ?>> {
        private Class<?> inner;
        private DataType<?> innerType;

        public MapReader(@NonNull AvroMap<?> type) {
            this.inner = type.getInner();
            this.innerType = AvroEntitySchema.getDataType(this.inner);
            Preconditions.checkNotNull(this.innerType);
        }

        /**
         * @param data
         * @return
         * @throws Exception
         */
        @Override
        public Map<String, ?> read(@NonNull Object data) throws Exception {
            Preconditions.checkNotNull(inner);
            Preconditions.checkArgument(data instanceof Map);
            return (Map<String, ?>) data;
        }

        /**
         * @return
         */
        @Override
        public Class<?> getType() {
            return Map.class;
        }
    }
}
