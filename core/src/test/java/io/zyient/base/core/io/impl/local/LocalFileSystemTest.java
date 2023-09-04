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

package io.zyient.base.core.io.impl.local;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.io.FileSystemManager;
import io.zyient.base.core.io.Reader;
import io.zyient.base.core.io.Writer;
import io.zyient.base.core.io.FileSystem;
import io.zyient.base.core.io.model.DirectoryInode;
import io.zyient.base.core.io.model.FileInode;
import io.zyient.base.core.utils.DemoEnv;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class LocalFileSystemTest {
    private static final String __CONFIG_FILE = "src/test/resources/local/fs-mapped-test.xml";
    private static final String FS_DEMO = "local-test-1";
    private static final String FS_DEMO_DOMAIN = "local-demo-1";

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
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            File d = new File(di.getAbsolutePath());
            assertTrue(d.exists() && d.isDirectory());
            DirectoryInode cdi = fs.mkdir(di, "child");
            assertNotNull(cdi);
            DefaultLogger.info(String.format("Created directory. [path=%s]", cdi.getAbsolutePath()));
            d = new File(cdi.getAbsolutePath());
            String cdir = String.format("%s/", cdi.getFsPath());
            cdi = (DirectoryInode) fs.getInode(cdi.getDomain(), cdir);
            assertNotNull(cdi);
            assertTrue(d.exists() && d.isDirectory());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }

    @Test
    void create() {
        try {
            String dir = String.format("demo/local/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            File d = new File(di.getAbsolutePath());
            assertTrue(d.exists() && d.isDirectory());
            String name = String.format("test/%s.tmp", UUID.randomUUID().toString());
            FileInode fi = fs.create(di, name);
            assertNotNull(fi);
            DefaultLogger.info(String.format("Created directory. [path=%s]", fi.getAbsolutePath()));
            d = new File(fi.getAbsolutePath());
            assertTrue(d.exists() && d.isFile());
            fi = (FileInode) fs.getInode(di.getDomain(), fi.getFsPath());
            assertNotNull(fi);

            assertTrue(fs.delete(di.getPathInfo(), true));
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
            File d = new File(di.getAbsolutePath());
            assertTrue(d.exists() && d.isDirectory());
            FileInode fi = fs.create(di, String.format("test/%s.tmp", UUID.randomUUID().toString()));

            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNotNull(fi);
            d = new File(fi.getAbsolutePath());
            assertTrue(d.exists() && d.isFile());

            assertTrue(fs.delete(fi.getPathInfo()));
            assertFalse(d.exists());

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
            String dir = String.format("demo/local/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            File d = new File(di.getAbsolutePath());
            assertTrue(d.exists() && d.isDirectory());
            FileInode fi = fs.create(di.getDomain(), String.format("test/%s.tmp", UUID.randomUUID().toString()));

            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNotNull(fi);
            d = new File(fi.getAbsolutePath());
            assertTrue(d.exists() && d.isFile());
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
            String dir = String.format("demo/local/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getAbsolutePath()));
            File d = new File(di.getAbsolutePath());
            assertTrue(d.exists() && d.isDirectory());
            FileInode fi = fs.create(di.getDomain(), String.format("test/%s.tmp", UUID.randomUUID().toString()));

            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNotNull(fi);
            d = new File(fi.getAbsolutePath());
            assertTrue(d.exists() && d.isFile());
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
            try (Writer writer = fs.writer(fi)) {
                for (int ii = 0; ii < 200; ii++) {
                    String str = String.format("[%s] Test write line [%d]...\n", UUID.randomUUID().toString(), ii);
                    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                    writer.write(bytes);
                    written += bytes.length;
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

            assertTrue(fs.delete(di.getPathInfo(), true));
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            DefaultLogger.error(ex.getLocalizedMessage());
            fail(ex);
        }
    }
}