package ai.sapper.cdc.entity;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.entity.avro.AvroEntitySchema;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.*;

class DataTypeTest {
    public static final DataType<String> LONGTEXT = new TextType("LONGTEXT", Types.LONGNVARCHAR, Integer.MAX_VALUE * 2L);

    @Test
    void testEquals() {
        try {
            DataType<?> dt = AvroEntitySchema.FLOAT;
            DataType<?> nt = new DataType<>("float", Float.TYPE, Types.FLOAT);
            assertEquals(nt, dt);

            dt = LONGTEXT;
            dt = new SizedDataType<>((SizedDataType<?>) dt, 200);
            assertTrue(dt instanceof TextType);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}