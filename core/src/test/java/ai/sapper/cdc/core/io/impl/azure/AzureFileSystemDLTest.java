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

class AzureFileSystemDLTest {
    private static final String __CONFIG_FILE = "src/test/resources/azure/fs-azure-dl-test.xml";
    private static final String FS_DEMO = "azure-dl-1";
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
            String dir = String.format("demo/azure/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            FileInode fi = fs.create(di, String.format("test/%s.tmp", UUID.randomUUID().toString()));
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
            String dir = String.format("demo/azure/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            FileInode fi = fs.create(di, String.format("test/%s.tmp", UUID.randomUUID().toString()));

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
            Thread.sleep(5000);
            assertTrue(fs.delete(fi.getPathInfo()));
            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNull(fi);

            assertTrue(fs.delete(di.getPathInfo(), true));
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }

    @Test
    void getReader() {
        try {
            String dir = String.format("demo/azure/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            FileInode fi = fs.create(di, String.format("test/%s.tmp", UUID.randomUUID().toString()));

            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNotNull(fi);
            long written = 0;
            try (Writer writer = fs.writer(fi)) {
                for (int ii = 0; ii < 200; ii++) {
                    String str = String.format("[%s] Test write line [%d]...\n", UUID.randomUUID().toString(), ii);
                    written += writer.write(str.getBytes(StandardCharsets.UTF_8));
                }
                writer.commit(true);
            }
            Thread.sleep(5000);

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
                assertEquals(written, size);
            }
            assertTrue(fs.delete(fi.getPathInfo()));
            assertTrue(fs.delete(di.getPathInfo(), true));
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }


    @Test
    void getWriter() {
        try {
            String dir = String.format("demo/azure/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            FileInode fi = fs.create(di, String.format("test/%s.tmp", UUID.randomUUID().toString()));

            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNotNull(fi);
            long written = 0;
            try (Writer writer = fs.writer(fi)) {
                for (int ii = 0; ii < 200; ii++) {
                    String str = String.format("[%s] Test write line [%d]...\n", UUID.randomUUID().toString(), ii);
                    written += writer.write(str.getBytes(StandardCharsets.UTF_8));
                }
                writer.commit(false);
            }
            Thread.sleep(5000);
            try (Writer writer = fs.writer(fi)) {
                for (int ii = 0; ii < 200; ii++) {
                    String str = String.format("[%s] Test write line [%d]...\n", UUID.randomUUID().toString(), ii);
                    written += writer.write(str.getBytes(StandardCharsets.UTF_8));
                }
                writer.commit(true);
            }
            Thread.sleep(5000);

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
                assertEquals(written, size);
            }
            assertTrue(fs.delete(fi.getPathInfo()));
            assertTrue(fs.delete(di.getPathInfo(), true));
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }
}