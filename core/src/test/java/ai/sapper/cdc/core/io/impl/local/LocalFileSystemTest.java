package ai.sapper.cdc.core.io.impl.local;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.core.io.FileSystemManager;
import ai.sapper.cdc.core.utils.DemoEnv;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LocalFileSystemTest {
    private static final String __CONFIG_FILE = "src/test/resources/fs-local-test.xml";
    private static XMLConfiguration xmlConfiguration = null;
    private static FileSystemManager manager;
    private static DemoEnv env = new DemoEnv();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.init(xmlConfiguration);
        manager = env.fileSystemManager();
        Preconditions.checkNotNull(manager);
    }

    @AfterAll
    public static void stop() throws Exception {
        env.close();
    }

    @Test
    void mkdir() {
    }

    @Test
    void mkdirs() {
    }

    @Test
    void create() {
    }

    @Test
    void delete() {
    }

    @Test
    void exists() {
    }

    @Test
    void getReader() {
    }

    @Test
    void getWriter() {
    }
}