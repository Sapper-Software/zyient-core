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

package io.zyient.core.filesystem.impl.mapped;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.FileSystemManager;
import io.zyient.core.filesystem.Reader;
import io.zyient.core.filesystem.Writer;
import io.zyient.core.filesystem.env.DemoFileSystemEnv;
import io.zyient.core.filesystem.model.DirectoryInode;
import io.zyient.core.filesystem.model.FileInode;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class MappedFileSystemTest {
    private static final String __CONFIG_FILE = "src/test/resources/mapped/fs-mapped-test.xml";
    private static final String FS_DEMO = "mapped-test-1";
    private static final String FS_DEMO_DOMAIN = "mapped-demo-1";

    private static XMLConfiguration xmlConfiguration = null;
    private static FileSystemManager manager;
    private static DemoFileSystemEnv env = new DemoFileSystemEnv();
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
    void getReader() {
        try {
            String dir = String.format("demo/local/%s", UUID.randomUUID().toString());
            DirectoryInode di = fs.mkdirs(FS_DEMO_DOMAIN, dir);
            assertNotNull(di);
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getPath()));
            File d = new File(di.getFsPath());
            assertTrue(d.exists() && d.isDirectory());
            FileInode fi = fs.create(di.getDomain(), String.format("test/%s.tmp", UUID.randomUUID().toString()));

            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNotNull(fi);
            d = new File(fi.getFsPath());
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
                byte[] buffer = new byte[1024];
                while (true) {
                    int s = reader.read(buffer);
                    if (s < 1024) {
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
            DefaultLogger.info(String.format("Created directory. [path=%s]", di.getPath()));
            File d = new File(di.getFsPath());
            assertTrue(d.exists() && d.isDirectory());
            FileInode fi = fs.create(di.getDomain(), String.format("test/%s.tmp", UUID.randomUUID().toString()));

            fi = (FileInode) fs.getInode(fi.getPathInfo());
            assertNotNull(fi);
            d = new File(fi.getFsPath());
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