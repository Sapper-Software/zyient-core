package io.zyient.base.core.keystore;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.utils.JavaKeyStoreUtil;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class AWSKeyStoreTest {
    private static final String __CONFIG_FILE = "src/test/resources/keystore/keystore-aws.xml";
    private static final String __CONFIG_PATH = "config";
    private static XMLConfiguration xmlConfiguration = null;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
    }

    @Test
    void read() {
        try {


            String keyName = "test";
            String keyValue = "test1234";
            String password = "testasb";

            JavaKeyStoreUtil util = new JavaKeyStoreUtil();
            util.setConfigFile(__CONFIG_FILE);
            util.setPassword(password);
            util.setKey(keyName);
            util.setValue(keyValue);
            util.run();

            util.setKey(UUID.randomUUID().toString());
            util.setValue("Dummy");
            util.run();

            KeyStore store = util.getEnv().keyStore();

            store.save(keyName, keyValue);
            store.flush();
            String v = store.read(keyName);
            assertEquals(keyValue, v);

            File file = ((JavaKeyStore)store).filePath(password);
            assertNotNull(file);
            //store.delete();
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }
}