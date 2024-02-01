package io.zyient.core.persistence.impl.rdbms.converters;

import com.google.common.base.Strings;
import io.zyient.base.common.utils.JSONUtils;
import jakarta.persistence.AttributeConverter;

public class ThrowableJsonConverter implements AttributeConverter<Throwable, String> {
    /**
     * Converts the value stored in the entity attribute into the
     * data representation to be stored in the database.
     *
     * @param attribute the entity attribute value to be converted
     * @return the converted data to be stored in the database
     * column
     */
    @Override
    public String convertToDatabaseColumn(Throwable attribute) {
        try {
            if (attribute != null)
                return JSONUtils.asString(attribute);
            return null;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Converts the data stored in the database column into the
     * value to be stored in the entity attribute.
     * Note that it is the responsibility of the converter writer to
     * specify the correct <code>dbData</code> type for the corresponding
     * column for use by the JDBC driver: i.e., persistence providers are
     * not expected to do such type conversion.
     *
     * @param dbData the data from the database column to be
     *               converted
     * @return the converted value to be stored in the entity
     * attribute
     */
    @Override
    public Throwable convertToEntityAttribute(String dbData) {
        try {
            if (!Strings.isNullOrEmpty(dbData)) {
                return JSONUtils.read(dbData, Throwable.class);
            }
            return null;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}