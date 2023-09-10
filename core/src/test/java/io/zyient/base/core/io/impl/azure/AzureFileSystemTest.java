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

package io.zyient.base.core.io.impl.azure;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DateTimeUtils;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.io.FileSystem;
import io.zyient.base.core.io.FileSystemManager;
import io.zyient.base.core.io.Reader;
import io.zyient.base.core.io.Writer;
import io.zyient.base.core.io.model.DirectoryInode;
import io.zyient.base.core.io.model.FileInode;
import io.zyient.base.core.utils.DemoEnv;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AzureFileSystemTest {
    private static final String __CONFIG_FILE = "src/test/resources/azure/fs-azure-test.xml";
    private static final String FS_DEMO = "azure-test-1";
    private static final String FS_DEMO_DOMAIN = "azure-demo-1";

    private static XMLConfiguration xmlConfiguration = null;
    private static FileSystemManager manager;
    private static DemoEnv env = new DemoEnv();
    private static FileSystem fs;
    private static String BASE_DIR;


    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.init(xmlConfiguration);
        manager = env.fileSystemManager();
        Preconditions.checkNotNull(manager);
        fs = manager.get(FS_DEMO);
        Preconditions.checkNotNull(fs);
        String dtDir = DateTimeUtils.formatTimestamp("yyyy/MM/dd/HH/mm");
        BASE_DIR = String.format("demo/azure/%s/%s", fs.getClass().getSimpleName(), dtDir);
    }

    @AfterAll
    public static void stop() throws Exception {
        env.close();
    }

    @Test
    void create() {
        try {
            String dir = String.format("%s/%s/create", BASE_DIR, UUID.randomUUID().toString());
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
            String dir = String.format("%s/%s/delete", BASE_DIR, UUID.randomUUID().toString());
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
                    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                    writer.write(bytes);
                    written += bytes.length;
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
            String dir = String.format("%s/%s/reader", BASE_DIR, UUID.randomUUID().toString());
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
                    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                    writer.write(bytes);
                    written += bytes.length;
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
            FileInode ti = fs.create(di, String.format("copied_%s.tmp", UUID.randomUUID().toString()));
            fs.copy(fi, ti.getPathInfo());
            assertTrue(ti.getPathInfo().exists());
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
            String dir = String.format("%s/%s/writer", BASE_DIR, UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            FileInode fi = fs.create(di, String.format("%s.tmp", UUID.randomUUID().toString()));

            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNotNull(fi);
            long written = 0;
            try (Writer writer = fs.writer(fi)) {
                for (int ii = 0; ii < 200; ii++) {
                    String str = String.format("[%s] Test write line [%d]...\n", UUID.randomUUID().toString(), ii);
                    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                    writer.write(bytes);
                    written += bytes.length;
                }
                writer.commit(false);
            }
            Thread.sleep(5000);
            try (Writer writer = fs.writer(fi)) {
                for (int ii = 0; ii < 200; ii++) {
                    String str = String.format("[%s] Test write line [%d]...\n", UUID.randomUUID().toString(), ii);
                    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                    writer.write(bytes);
                    written += bytes.length;
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
            FileInode ti = fs.create(di, String.format("moved_%s.tmp", UUID.randomUUID().toString()));
            fs.move(fi, ti.getPathInfo());
            assertTrue(ti.getPathInfo().exists());
            assertFalse(fi.getPathInfo().exists());
            assertTrue(fs.delete(di.getPathInfo(), true));
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }
}