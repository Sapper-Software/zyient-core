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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.cdc.entity.jdbc.DbEntitySchema;
import io.zyient.cdc.entity.schema.EntitySchema;
import io.zyient.cdc.entity.schema.SchemaEntity;
import io.zyient.cdc.entity.schema.SchemaField;
import io.zyient.cdc.entity.types.BinaryType;
import io.zyient.cdc.entity.types.DataType;
import io.zyient.cdc.entity.types.TextType;
import lombok.NonNull;
import org.apache.avro.Schema;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;

public class JdbcSchemaGenerator extends EntitySchemaGenerator {
    /**
     * @param schemaEntity
     * @return
     * @throws Exception
     */
    @Override
    public EntitySchema generate(@NonNull SchemaEntity schemaEntity,
                                 @NonNull SchemaEntity targetEntity,
                                 Object... params) throws Exception {
        Preconditions.checkNotNull(connection());
        Connection connection = connection().getConnection();
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet columns = metaData.getColumns(null, targetEntity.getDomain(), targetEntity.getEntity(), null)) {
            DbEntitySchema schema = new DbEntitySchema();
            schema.setSchemaEntity(schemaEntity);
            schema.setNamespace(schemaEntity.getDomain());
            schema.setName(schemaEntity.getEntity());

            while (columns.next()) {
                String cName = columns.getString("COLUMN_NAME");
                int cDatatype = columns.getInt("DATA_TYPE");
                String cSize = columns.getString("COLUMN_SIZE");
                int cPosition = columns.getInt("ORDINAL_POSITION");
                boolean cIsNullable = columns.getBoolean("IS_NULLABLE");
                Integer size = null;
                if (!Strings.isNullOrEmpty(cSize)) {
                    size = Integer.parseInt(cSize);
                }
                DataType<?> dataType = parseDataType(cDatatype, size);
                SchemaField field = new SchemaField();
                field.setName(cName);
                field.setPosition(cPosition);
                field.setNullable(cIsNullable);
                field.setDataType(dataType);
                field.setJdbcType(cDatatype);
                schema.add(field);
            }
            Schema sc = schema.generateSchema();
            if (sc == null) {
                throw new Exception(
                        String.format("Error generating AVRO schema. [entity=%s]", schemaEntity.toString()));
            }
            return schema;
        }
    }


    /**
     * @param dataType
     * @param size
     * @return
     * @throws Exception
     */
    public static DataType<?> parseDataType(int dataType, Integer size) throws Exception {
        if (dataType == Types.BIGINT) {
            return DbEntitySchema.BIGINT;
        } else if (dataType == Types.ARRAY) {
            return DbEntitySchema.OBJECT;
        } else if (dataType == Types.BOOLEAN ||
                dataType == Types.BIT) {
            return DbEntitySchema.BOOLEAN;
        } else if (dataType == Types.FLOAT) {
            return DbEntitySchema.FLOAT;
        } else if (dataType == Types.DOUBLE) {
            return DbEntitySchema.DOUBLE;
        } else if (dataType == Types.BINARY
                || dataType == Types.VARBINARY) {
            return new BinaryType(DbEntitySchema.VARBINARY.getName(), Types.VARBINARY, size);
        } else if (dataType == Types.BLOB) {
            return DbEntitySchema.BLOB;
        } else if (dataType == Types.DECIMAL) {
            return DbEntitySchema.DOUBLE;
        } else if (dataType == Types.INTEGER) {
            return DbEntitySchema.INTEGER;
        } else if (dataType == Types.LONGNVARCHAR ||
                dataType == Types.LONGVARCHAR) {
            return DbEntitySchema.LONGTEXT;
        } else if (dataType == Types.LONGVARBINARY) {
            return DbEntitySchema.LONGBLOB;
        } else if (dataType == Types.REAL) {
            return DbEntitySchema.DOUBLE;
        } else if (dataType == Types.SMALLINT) {
            return DbEntitySchema.SMALLINT;
        } else if (dataType == Types.TINYINT) {
            return DbEntitySchema.TINYINT;
        } else if (dataType == Types.CHAR ||
                dataType == Types.NCHAR ||
                dataType == Types.NVARCHAR
                || dataType == Types.VARCHAR) {
            return new TextType(DbEntitySchema.VARCHAR.getName(), Types.VARCHAR, size);
        } else if (dataType == Types.DATE) {
            return DbEntitySchema.DATE;
        } else if (dataType == Types.TIME ||
                dataType == Types.TIME_WITH_TIMEZONE) {
            return DbEntitySchema.TIME;
        } else if (dataType == Types.TIMESTAMP ||
                dataType == Types.TIMESTAMP_WITH_TIMEZONE) {
            return DbEntitySchema.TIMESTAMP;
        }
        return DbEntitySchema.OBJECT;
    }
}
