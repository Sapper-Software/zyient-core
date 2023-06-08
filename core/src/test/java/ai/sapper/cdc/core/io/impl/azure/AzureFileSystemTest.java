package ai.sapper.cdc.core.io.impl.azure;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.common.model.services.EConfigFileType;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.FileSystemManager;
import ai.sapper.cdc.core.io.Reader;
import ai.sapper.cdc.core.io.Writer;
import ai.sapper.cdc.core.io.model.DirectoryInode;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.utils.DemoEnv;
import com.google.common.base.Preconditions;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AzureFileSystemTest {
    private static final String __CONFIG_FILE = "src/test/resources/fs-azure-test.xml";
    private static final String FS_DEMO = "azure-test-1";
    private static final String FS_DEMO_DOMAIN = "azure-demo-1";

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
    void create() {
        try {
            String dir = String.format("demo/local/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            FileInode fi = fs.create(di.getDomain(), String.format("test/%s.tmp", UUID.randomUUID().toString()));
            assertNotNull(fi);
            DefaultLogger.info(String.format("Created file. [path=%s]", fi.getAbsolutePath()));

        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }

    @Test
    void delete() {
        try {
            String dir = String.format("demo/local/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            FileInode fi = fs.create(di.getDomain(), String.format("test/%s.tmp", UUID.randomUUID().toString()));

            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNotNull(fi);

            assertTrue(fs.delete(fi.getPathInfo()));
            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNull(fi);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }

    @Test
    void getReader() {
        try {
            String dir = String.format("demo/local/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            FileInode fi = fs.create(di.getDomain(), String.format("test/%s.tmp", UUID.randomUUID().toString()));

            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNotNull(fi);
            long written = 0;
            try (Writer writer = fs.writer(fi)) {
                for (int ii = 0; ii < 200; ii++) {
                    String str = String.format("[%s] Test write line [%d]...\n", UUID.randomUUID().toString(), ii);
                    written = writer.write(str.getBytes(StandardCharsets.UTF_8));
                }
                writer.commit(true);
            }
            try (Reader reader = fs.reader(fi)) {
                int size = 0;
                byte[] buffer = new byte[512];
                while (true) {
                    int s = reader.read(buffer);
                    if (s < 512) {
                        size += s;
                        break;
                    }
                    size += s;
                }
                assertEquals(size, written);
            }
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }

    @Test
    void getWriter() {
    }
}