package ai.sapper.cdc.core.io.impl.local;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.FileSystemManager;
import ai.sapper.cdc.core.io.model.DirectoryInode;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.utils.DemoEnv;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class LocalFileSystemTest {
    private static final String __CONFIG_FILE = "src/test/resources/fs-local-test.xml";
    private static final String FS_DEMO = "local-test-1";
    private static final String FS_DEMO_DOMAIN = "demo-1";

    private static XMLConfiguration xmlConfiguration = null;
    private static FileSystemManager manager;
    private static DemoEnv env = new DemoEnv();
    private static FileSystem fs;

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.init(xmlConfiguration);
        manager = env.fileSystemManager();
        Preconditions.checkNotNull(manager);
        fs = manager.get(FS_DEMO);
        Preconditions.checkNotNull(fs);
    }

    @AfterAll
    public static void stop() throws Exception {
        env.close();
    }

    @Test
    void mkdir() {
        try {
            String dir = String.format("demo/local/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.LOGGER.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            File d = new File(di.getAbsolutePath());
            assertTrue(d.exists() && d.isDirectory());
            DirectoryInode cdi = fs.mkdir(di, "child");
            assertNotNull(cdi);
            DefaultLogger.LOGGER.info(String.format("Created directory. [path=%s]", cdi.getAbsolutePath()));
            d = new File(cdi.getAbsolutePath());
            assertTrue(d.exists() && d.isDirectory());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }

    @Test
    void create() {
        try {
            String dir = String.format("demo/local/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.LOGGER.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            File d = new File(di.getAbsolutePath());
            assertTrue(d.exists() && d.isDirectory());
            FileInode fi = fs.create(di.getDomain(), String.format("test/%s.tmp", UUID.randomUUID().toString()));
            assertNotNull(fi);
            DefaultLogger.LOGGER.info(String.format("Created directory. [path=%s]", fi.getAbsolutePath()));
            d = new File(fi.getAbsolutePath());
            assertTrue(d.exists() && d.isFile());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.LOGGER.error(ex.getLocalizedMessage());
            fail(ex);
        }
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