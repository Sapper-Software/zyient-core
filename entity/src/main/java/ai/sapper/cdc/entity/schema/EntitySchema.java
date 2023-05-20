package ai.sapper.cdc.entity.schema;

import ai.sapper.cdc.common.schema.SchemaEntity;
import ai.sapper.cdc.common.schema.SchemaHelper;
import ai.sapper.cdc.common.schema.SchemaVersion;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.entity.*;
import ai.sapper.cdc.entity.avro.AvroEntitySchema;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.ObjectUtils;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.*;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public abstract class EntitySchema {
    public static final DataType<Object> OBJECT = new DataType<>("OBJECT", Object.class, Types.STRUCT);
    public static final DataType<Boolean> BOOLEAN = new DataType<>("BOOLEAN", Boolean.class, Types.BOOLEAN);
    public static final DataType<Integer> SHORT = new IntegerType("SHORT", Short.MIN_VALUE, Short.MAX_VALUE, Types.SMALLINT);
    public static final DataType<Integer> INTEGER = new IntegerType("INTEGER", Integer.MIN_VALUE, Integer.MAX_VALUE, Types.INTEGER);
    public static final DataType<Long> LONG = new NumericType<>("BIGINT", Long.class, Types.BIGINT);
    public static final DataType<Float> FLOAT = new DecimalType<>("FLOAT", Float.class, Types.FLOAT);
    public static final DataType<Double> DOUBLE = new DecimalType<>("DOUBLE", Double.class, Types.DOUBLE);
    public static final DataType<String> CHAR = new TextType("CHAR", Types.CHAR, 256);
    public static final DataType<ByteBuffer> BINARY = new BinaryType("BINARY", Types.BINARY, 256);
    public static final DataType<String> STRING = new TextType("STRING", Types.VARCHAR, 64 * 1024);
    public static final DataType<String> JSON = new TextType("JSON", -16, 2147483647L);
    public static final DataType<ObjectUtils.Null> NULL = new DataType<>("NULL", ObjectUtils.Null.class, Types.NULL);

    @Getter
    @Setter
    public static class UnionField {
        private Schema schema;
        private boolean nullable = false;
    }

    private static final String NONE = "NONE";
    public static final String DEFAULT_RECORD_FIELD = "JSON_MAPPING";

    @JsonIgnore
    private Schema schema;
    @JsonIgnore
    private SchemaEntity schemaEntity;
    private String entityZkPath;
    private String schemaStr;
    private SchemaVersion version;
    private String namespace;
    private String name;
    private long updatedTime;
    private String zkPath;
    private DataType<?> recordType;
    private Map<String, RecordSchemaField> recordTypes = new LinkedHashMap<>();
    private Map<String, SchemaField> fields;
    private Class<? extends CustomDataTypeMapper> mapperClass;
    @JsonIgnore
    private CustomDataTypeMapper mapper;

    public EntitySchema(@NonNull DataType<?> recordType) {
        this.recordType = recordType;
    }

    public EntitySchema(@NonNull EntitySchema source) {
        this.schema = source.schema;
        this.schemaEntity = source.schemaEntity;
        this.schemaStr = source.schemaStr;
        this.version = source.version;
        this.namespace = source.namespace;
        this.name = source.name;
        this.updatedTime = source.updatedTime;
        this.zkPath = source.zkPath;
        this.recordType = source.recordType;
        this.recordTypes = source.recordTypes;
        this.mapper = source.mapper;
        if (this.mapper != null) {
            this.mapperClass = this.mapper.getClass();
        }
        if (source.fields != null && !source.fields.isEmpty()) {
            for (SchemaField sf : source.fields.values()) {
                add(sf);
            }
        }
        this.entityZkPath = source.entityZkPath;
    }

    public EntitySchema withMapper(CustomDataTypeMapper mapper) {
        this.mapper = mapper;
        if (mapper != null) {
            this.mapperClass = this.mapper.getClass();
        }
        return this;
    }

    @JsonIgnore
    public ImmutableCollection<SchemaField> getFieldList() {
        if (fields != null && !fields.isEmpty()) {
            return ImmutableList.copyOf(fields.values());
        }
        return null;
    }

    @JsonIgnore
    public List<String> getFieldNames() {
        if (fields != null && !fields.isEmpty()) {
            List<String> names = new ArrayList<>(fields.size());
            names.addAll(fields.keySet());
            return ImmutableList.copyOf(names);
        }
        return null;
    }

    @JsonIgnore
    public List<String> getUpdateFields() {
        if (fields != null && !fields.isEmpty()) {
            List<String> names = new ArrayList<>(fields.size());
            for (String name : fields.keySet()) {
                SchemaField sf = fields.get(name);
                if (sf instanceof KeyField) {
                    continue;
                }
                names.add(name);
            }
            return ImmutableList.copyOf(names);
        }
        return null;
    }


    public Map<String, KeyField> keyFields() {
        Map<String, KeyField> keys = new HashMap<>();
        for (String name : getFields().keySet()) {
            SchemaField field = get(name);
            if (field instanceof KeyField) {
                keys.put(field.getName(), (KeyField) field);
            }
        }
        if (!keys.isEmpty()) return keys;
        return null;
    }

    public RecordSchemaField checkRecordType(@NonNull RecordSchemaField field) {
        for (String key : recordTypes.keySet()) {
            RecordSchemaField f = recordTypes.get(key);
            if (f.equals(field)) {
                return f;
            }
        }
        String key = String.format("%s.%s", field.getNamespace(), field.getName());
        recordTypes.put(key, field);
        return field;
    }

    public SchemaField get(@NonNull String name) {
        if (fields != null) return fields.get(name);
        return null;
    }

    public SchemaField getJdbcField(@NonNull String name) {
        name = name.toUpperCase();
        SchemaField sf = get(name);
        if (sf != null && sf.isDeleted()) return null;
        return sf;
    }

    public EntitySchema withSchema(@NonNull Schema schema,
                                   boolean parseSchema) throws Exception {
        return withSchema(schema, parseSchema, false);
    }

    public EntitySchema withSchema(@NonNull Schema schema,
                                   boolean parseSchema,
                                   boolean deep) throws Exception {
        if (parseSchema)
            parse(schema, deep);
        setSchema(schema);
        schemaStr = schema.toString(false);
        return this;
    }

    public void load() throws Exception {
        if (!Strings.isNullOrEmpty(schemaStr)) {
            Schema schema = new Schema.Parser().parse(schemaStr);
            withSchema(schema, false);
        }
        if (mapperClass != null) {
            mapper = mapperClass.getDeclaredConstructor().newInstance();
        }
    }

    private void parse(Schema schema, boolean deep) {
        setNamespace(schema.getNamespace());
        setName(schema.getName());
        List<Schema.Field> fs = schema.getFields();
        if (fs != null && !fs.isEmpty()) {
            for (Schema.Field f : fs) {
                SchemaField sf = readField(f, deep);
                if (sf != null) {
                    add(sf);
                }
            }
        }
    }

    private SchemaField readField(Schema.Field field, boolean deep) {
        DataType<?> dt = parseDataType(field);
        if (dt != null) {
            SchemaField sf = null;
            if (dt.equals(recordType)) {
                sf = new RecordSchemaField();
            } else
                sf = new SchemaField();
            String name = SchemaHelper.checkFieldName(field.name());
            name = name.toUpperCase();
            sf.setName(name);
            sf.setDataType(dt);
            sf.setDefaultVal(defaultVal(field));
            sf.setPosition(field.pos());
            UnionField uf = checkUnionType(field);
            if (uf != null) {
                sf.setNullable(uf.nullable);
            }
            if (deep && dt.equals(recordType)) {
                RecordSchemaField rf = (RecordSchemaField) sf;
                Schema schema = field.schema();
                List<Schema.Field> fs = schema.getFields();
                if (fs != null && !fs.isEmpty()) {
                    for (Schema.Field f : fs) {
                        SchemaField isf = readField(f, true);
                        if (isf != null) {
                            rf.add(isf);
                        }
                    }
                }
            }
            return sf;
        } else {
            DefaultLogger.warn(String.format("AVRO Datatype not supported: [type=%s]",
                    field.schema().getName()));
        }
        return null;
    }

    public static UnionField checkUnionType(@NonNull Schema.Field field) {
        Schema.Type type = field.schema().getType();
        Schema s = null;
        boolean nullable = false;
        if (type == Schema.Type.UNION) {
            List<Schema> fs = field.schema().getTypes();
            Schema.Type ut = null;
            for (Schema f : fs) {
                if (f.getType() == Schema.Type.NULL) {
                    nullable = true;
                    continue;
                }
                if (ut == null) {
                    ut = f.getType();
                    s = f;
                } else if (ut != f.getType()) {
                    s = null;
                    break;
                }
            }
            if (s != null) {
                UnionField uf = new UnionField();
                uf.schema = s;
                uf.nullable = nullable;

                return uf;
            }
        }
        return null;
    }

    public abstract DataType<?> parseDataType(@NonNull Schema.Field field);

    public abstract DataType<?> parseDataType(@NonNull Class<?> type);

    public abstract DataType<?> parseDataType(@NonNull Metadata.ColumnDef column) throws Exception;

    public abstract Schema generateSchema() throws Exception;

    public abstract Schema generateSchema(boolean deep) throws Exception;

    public EntityDiff diff(@NonNull EntitySchema target) throws Exception {
        EntityDiff diff = new EntityDiff();
        for (String name : getFields().keySet()) {
            SchemaField sf = get(name);
            SchemaField tf = target.get(name);
            if (tf == null) {
                diff.put(sf, ESchemaOp.DROP);
            } else if (!sf.equals(tf)) {
                diff.put(tf, ESchemaOp.MODIFY).current(sf);
            }
        }
        for (String name : target.getFields().keySet()) {
            SchemaField sf = target.get(name);
            SchemaField tf = get(name);
            if (tf == null) {
                diff.put(sf, ESchemaOp.ADD);
            }
        }
        if (!diff.isEmpty()) return diff;
        return null;
    }

    public void apply(@NonNull EntityDiff diff) throws Exception {
        apply(diff, true);
    }

    public void apply(@NonNull EntityDiff diff,
                      boolean ignoreDrop) throws Exception {
        if (diff.diff() != null && !diff.diff().isEmpty()) {
            for (DiffElement de : diff.diff().values()) {
                switch (de.op()) {
                    case ADD:
                        add(de.field());
                        break;
                    case MODIFY:
                        SchemaField sf = get(de.field().getName());
                        if (sf == null)
                            throw new Exception(
                                    String.format("Schema Field not found. [field=%s]", de.field().getName()));
                        remove(de.field().getName());
                        add(de.field());
                        break;
                    case DROP:
                        SchemaField rf = get(de.field().getName());
                        if (rf == null)
                            throw new Exception(
                                    String.format("Schema Field not found. [field=%s]", de.field().getName()));
                        if (!ignoreDrop) {
                            remove(de.field().getName());
                        }
                        break;
                }
            }
        }
    }

    public void update(@NonNull EntityDiff diff,
                       @NonNull BaseSchemaMapping mapping,
                       @NonNull CDCSchemaEntity entity) throws Exception {
        if (diff.diff() != null && !diff.diff().isEmpty()) {
            for (String key : diff.diff().keySet()) {
                DiffElement de = diff.get(key);
                checkSchemaOp(de.op(), de.field(), mapping, entity);
            }
        }
    }

    public boolean checkSchemaOp(@NonNull ESchemaOp op,
                                 @NonNull SchemaField field,
                                 @NonNull BaseSchemaMapping mapping,
                                 @NonNull CDCSchemaEntity entity) throws Exception {
        switch (op) {
            case ADD:
                return addSchemaOp(field, mapping);
            case DROP:
                return dropSchemaOp(field, mapping, entity);
            case MODIFY:
                return modifySchemaOp(field, mapping);
            default:
                throw new Exception(String.format("Operation not supported. [op=%s]", op.name()));
        }
    }

    private boolean addSchemaOp(@NonNull SchemaField field,
                                @NonNull BaseSchemaMapping mapping) throws Exception {
        SchemaField sf = mapping.getTargetField(field.getName());
        if (sf != null) {
            if (!sf.isDeleted()) {
                throw new Exception(String.format("Duplicate field: Field already exists. [name=%s]", field.getName()));
            } else {
                DataType<?> dt = parseDataType(field.getDataType().getJavaType());
                if (dt == null) {
                    throw new Exception(
                            String.format("Data Type not supported. [type=%s]", field.getDataType().getName()));
                }
                if (!dt.equals(sf.getDataType())) {
                    throw new Exception(
                            String.format("Previously deleted field has new datatype. [field=%s]", field.getName()));
                }
                sf.setDeleted(false);
            }
        } else {
            DataType<?> dt = parseDataType(field.getDataType().getJavaType());
            if (dt == null) {
                throw new Exception(
                        String.format("Data Type not supported. [type=%s]", field.getDataType().getName()));
            }
            field.setDataType(dt);
            add(field);
        }
        field.setDefaultVal(null);
        return true;
    }

    private boolean dropSchemaOp(@NonNull SchemaField field,
                                 @NonNull BaseSchemaMapping mapping,
                                 @NonNull CDCSchemaEntity entity) throws Exception {
        SchemaField sf = mapping.getTargetField(field.getName());
        if (sf == null) return false;
        if (entity.getOptions().ignoreDroppedColumn()) {
            sf.setDeleted(true);
        } else {
            fields.remove(sf.getName());
            mapping.getMappings().remove(field.getName());
        }
        return true;
    }

    private boolean modifySchemaOp(@NonNull SchemaField field,
                                   @NonNull BaseSchemaMapping mapping) throws Exception {
        SchemaField sf = mapping.getTargetField(field.getName());
        if (sf == null) return false;
        boolean ret = false;
        DataType<?> dt = parseDataType(field.getDataType().getJavaType());
        if (dt == null) {
            throw new Exception(
                    String.format("Data Type not supported. [type=%s]", field.getDataType().getName()));
        }
        if (!dt.equals(sf.getDataType())) {
            sf.setDataType(dt);
            field.setDataType(dt);
            ret = true;
        }
        if (field.isNullable() != sf.isNullable()) {
            sf.setNullable(field.isNullable());
            ret = true;
        }
        return ret;
    }

    public EntitySchema add(@NonNull SchemaField field) {
        if (fields == null) {
            fields = new HashMap<>();
        }
        String key = field.getName().toUpperCase();
        fields.put(key, field);

        return this;
    }

    public EntitySchema add(@NonNull String name,
                            @NonNull DataType<?> dataType,
                            Object defaultVal) {
        SchemaField field = new SchemaField();
        field.setName(name);
        field.setDataType(dataType);
        field.setDefaultVal(defaultVal);

        return add(field);
    }

    public EntitySchema add(@NonNull String name,
                            @NonNull DataType<?> dataType) {
        return add(name, dataType, null);
    }

    public SchemaField remove(String name) {
        if (fields != null)
            return fields.remove(name);
        return null;
    }

    public EntitySchema union(@NonNull EntitySchema target) {
        if (target.fields != null) {
            for (String fn : target.fields.keySet()) {
                if (!fields.containsKey(fn)) {
                    add(target.fields.get(fn));
                }
            }
        }
        return this;
    }

    public void toUpperCase() {
        if (!Strings.isNullOrEmpty(name))
            name = name.toUpperCase();
        if (!Strings.isNullOrEmpty(namespace))
            namespace = namespace.toUpperCase();

        if (fields != null && !fields.isEmpty()) {
            Map<String, SchemaField> fs = new HashMap<>(fields.size());
            for (String name : fields.keySet()) {
                SchemaField sf = fields.get(name);
                sf.setName(sf.getName().toUpperCase());
                fs.put(sf.getName(), sf);
            }
            fields = fs;
        }
    }

    public Object defaultVal(Schema.Field field) {
        return null;
    }

    public Schema getMappingRecord(String namespace, String name) {
        return SchemaBuilder.record(name)
                .namespace(namespace)
                .fields()
                .name(DEFAULT_RECORD_FIELD)
                .type(Schema.create(Schema.Type.STRING))
                .noDefault()
                .endRecord();
    }

    public Schema getArrayRecord(@NonNull ArraySchemaField field,
                                 @NonNull Schema fs) {
        return SchemaBuilder.array()
                .items(fs);
    }

    public Schema getMapRecord(@NonNull MapSchemaField field,
                               @NonNull Schema fs) {
        return SchemaBuilder.map()
                .values(fs);
    }

    public Schema getSchemaField(@NonNull DataType<?> type) {
        Schema.Type st = AvroEntitySchema.getAvroBasicType(type);
        if (st != null) {
            if (st == Schema.Type.BOOLEAN) {
                return SchemaBuilder.builder().booleanType();
            } else if (st == Schema.Type.INT) {
                return SchemaBuilder.builder().intType();
            } else if (st == Schema.Type.LONG) {
                return SchemaBuilder.builder().longType();
            } else if (st == Schema.Type.FLOAT) {
                return SchemaBuilder.builder().floatType();
            } else if (st == Schema.Type.DOUBLE) {
                return SchemaBuilder.builder().doubleType();
            } else if (st == Schema.Type.STRING || st == Schema.Type.ENUM) {
                return SchemaBuilder.builder().stringType();
            }
        }
        return null;
    }

    public Schema generateComplexSchema(CustomDataTypeMapper mapper) throws Exception {
        SchemaBuilder.BaseTypeBuilder<
                SchemaBuilder.UnionAccumulator<Schema>> root = SchemaBuilder.unionOf();
        for (String key : getRecordTypes().keySet()) {
            RecordSchemaField record = getRecordTypes().get(key);
            Schema schema = generateSchema(record, mapper);
            root = root.type(schema).and();
        }
        SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(getName());
        builder.namespace(getNamespace());
        SchemaBuilder.FieldAssembler<Schema> fields = builder.fields();
        for (String name : getFields().keySet()) {
            SchemaField field = get(name);
            fields = addSchemaField(field, fields, mapper, false);
        }
        Schema avro = root.type(fields.endRecord()).endUnion();
        withSchema(avro, false);
        return getSchema();
    }

    private Schema generateSchema(RecordSchemaField record,
                                  CustomDataTypeMapper mapper) throws Exception {
        SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(record.getName());
        builder.namespace(getNamespace());
        SchemaBuilder.FieldAssembler<Schema> fields = builder.fields();
        for (String name : record.getFields().keySet()) {
            SchemaField field = record.getFields().get(name);
            if (field instanceof RecordSchemaField) {
                RecordSchemaField r = (RecordSchemaField) field;
                fields = fields.name(field.getName())
                        .type(String.format("%s.%s", getNamespace(), r.getName()))
                        .noDefault();
            } else
                fields = addSchemaField(field, fields, mapper, false);
        }

        return fields.endRecord();
    }

    private Schema parseRecordField(RecordSchemaField record,
                                    String name,
                                    CustomDataTypeMapper mapper,
                                    boolean deep) throws Exception {

        SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(name);
        builder.namespace(getNamespace());
        SchemaBuilder.FieldAssembler<Schema> fields = builder.fields();
        for (String fname : record.getFields().keySet()) {
            SchemaField field = record.getFields().get(fname);
            if (field instanceof RecordSchemaField) {
                RecordSchemaField r = (RecordSchemaField) field;
                fields = fields.name(field.getName())
                        .type(String.format("%s.%s", getNamespace(), r.getName()))
                        .noDefault();
            } else
                fields = addSchemaField(field, fields, mapper, deep);
        }
        return fields.endRecord();
    }

    public SchemaBuilder.FieldAssembler<Schema> addSchemaField(SchemaField field,
                                                                SchemaBuilder.FieldAssembler<Schema> fields,
                                                                CustomDataTypeMapper mapper,
                                                                boolean deep) throws Exception {
        Schema.Type st = null;
        if (mapper == null)
            st = parseAvroField(field.getDataType());
        else
            st = parseAvroField(field.getDataType(), mapper);
        if (st == null) {
            throw new Exception(
                    String.format("DataType not supported. [field=%s][type=%s]",
                            field.getName(), field.getDataType().getName()));
        }
        if (st == Schema.Type.BOOLEAN) {
            if (field.isNullable())
                fields = fields.optionalBoolean(field.getName());
            else
                fields = fields.requiredBoolean(field.getName());
        } else if (st == Schema.Type.INT) {
            if (field.isNullable())
                fields = fields.optionalInt(field.getName());
            else
                fields = fields.requiredInt(field.getName());
        } else if (st == Schema.Type.LONG) {
            if (field.isNullable())
                fields = fields.optionalLong(field.getName());
            else
                fields = fields.requiredLong(field.getName());
        } else if (st == Schema.Type.FLOAT) {
            if (field.isNullable())
                fields = fields.optionalFloat(field.getName());
            else
                fields = fields.requiredFloat(field.getName());
        } else if (st == Schema.Type.DOUBLE) {
            if (field.isNullable())
                fields = fields.optionalDouble(field.getName());
            else
                fields = fields.requiredDouble(field.getName());
        } else if (st == Schema.Type.STRING) {
            if (field.isNullable())
                fields = fields.optionalString(field.getName());
            else
                fields = fields.requiredString(field.getName());
        } else if (st == Schema.Type.BYTES) {
            if (field.isNullable()) {
                fields = fields.optionalBytes(field.getName());
            } else {
                fields = fields.requiredBytes(field.getName());
            }
        } else if (st == Schema.Type.ARRAY && (field instanceof ArraySchemaField)) {
            ArraySchemaField af = (ArraySchemaField) field;
            Schema fs = getSchemaField(af.getField().getDataType());
            if (fs == null) {
                SchemaField inner = af.getField();
                fs = getComplexSchema(inner, mapper, deep);
            }
            Schema as = getArrayRecord(af, fs);
            fields = fields.name(field.getName())
                    .type(as)
                    .noDefault();
        } else if (st == Schema.Type.MAP && (field instanceof MapSchemaField)) {
            MapSchemaField af = (MapSchemaField) field;
            Schema fs = getSchemaField(af.getValueField().getDataType());
            if (fs == null) {
                fs = getComplexSchema(af.getValueField(), mapper, deep);
            }
            Schema as = getMapRecord(af, fs);
            fields = fields.name(field.getName())
                    .type(as)
                    .noDefault();
        } else {
            if (st == Schema.Type.RECORD && (field instanceof RecordSchemaField)) {
                if (deep) {
                    Schema fieldSchema = parseRecordField((RecordSchemaField) field, field.getName(), mapper, true);
                    fields = fields.name(field.getName()).type(fieldSchema).noDefault();
                } else {
                    RecordSchemaField r = (RecordSchemaField) field;
                    fields = fields.name(field.getName()).type(String.format("%s.%s", this.getNamespace(), r.getName())).noDefault();
                }
            } else
                fields = fields.name(field.getName())
                        .type(getMappingRecord(getNamespace(), field.getName())).noDefault();
        }
        return fields;
    }

    public @NonNull Schema getComplexSchema(SchemaField field,
                                            CustomDataTypeMapper mapper,
                                            boolean deep) throws Exception {
        if (field instanceof RecordSchemaField) {
            return parseRecordField((RecordSchemaField) field, field.getName(), mapper, deep);
        } else if (field instanceof MapSchemaField) {
            Schema fs = getSchemaField(((MapSchemaField) field).getValueField().getDataType());
            if (fs == null) {
                fs = getComplexSchema(((MapSchemaField) field).getValueField(), mapper, deep);
            }
            return SchemaBuilder.map().values(fs);
        } else if (field instanceof ArraySchemaField) {
            Schema fs = getSchemaField(((ArraySchemaField) field).getField().getDataType());
            if (fs == null) {
                fs = getComplexSchema(((ArraySchemaField) field).getField(), mapper, deep);
            }
            return SchemaBuilder.array().items(fs);
        } else if (field.getDataType().equals(NULL)) {
            return SchemaBuilder.builder().nullType();
        }
        throw new Exception(
                String.format("Complex Field type not supported. [class=%s][type=%s]",
                        field.getClass().getCanonicalName(), field.getDataType()));
    }

    public Schema.Type parseAvroField(@NonNull DataType<?> dbDataType) {
        Schema.Type st = AvroEntitySchema.getAvroBasicType(dbDataType);
        if (st == null) {
            if (dbDataType.equals(ArraySchemaField.ARRAY)) {
                return Schema.Type.ARRAY;
            } else if (dbDataType.equals(MapSchemaField.MAP)) {
                return Schema.Type.MAP;
            } else if (dbDataType.equals(OBJECT) || dbDataType.equals(RecordSchemaField.RECORD)) {
                return Schema.Type.RECORD;
            } else if (dbDataType.equals(NULL)) {
                return Schema.Type.NULL;
            }
        }
        return st;
    }

    public Schema.Type parseAvroField(@NonNull DataType<?> dbDataType,
                                      @NonNull CustomDataTypeMapper mapper) {
        Schema.Type st = mapper.getAvroType(dbDataType);
        if (st != null) return st;
        return parseAvroField(dbDataType);
    }
}
