/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
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

package io.zyient.core.extraction.azure;

import com.azure.ai.formrecognizer.documentanalysis.models.AnalyzeResult;
import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.core.extraction.env.DemoDataStoreEnv;
import io.zyient.core.extraction.model.Source;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class DocIExtractorTest {
    private static final String __CONFIG_FILE = "src/test/resources/azure/test-doci-env.xml";
    private static final String __INPUT_FILE_1 = "src/test/resources/input/sec-filing-sample-01.pdf";
    private static final String __INPUT_FILE_2 = "src/test/resources/input/1699822049.pdf";


    private static XMLConfiguration xmlConfiguration = null;
    private static final DemoDataStoreEnv env = new DemoDataStoreEnv();
    private static final DocIExtractor extractor = new DocIExtractor();

    @BeforeAll
    public static void setup() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.create(xmlConfiguration);
        env.connectionManager().save();
        HierarchicalConfiguration<ImmutableNode> config = env.demoConfig().configurationAt("extraction");
        extractor.configure(config, env);
    }

    @AfterAll
    public static void stop() throws Exception {
        env.close();
    }


    @Test
    void extract() {
        try {
            File input = new File(__INPUT_FILE_2);
            AnalyzeResult result = extractor.extract(input, null);
            assertNotNull(result);
            String name = FilenameUtils.getName(input.getAbsolutePath());
            File dir = PathUtils.getTempDir("zyient/extraction/azure");
            File path = new File(PathUtils.formatPath(String.format("%s/%s.json",
                    dir.getAbsolutePath(), name)));
            try (FileOutputStream os = new FileOutputStream(path)) {
                String json = JSONUtils.asString(result);
                os.write(json.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
            DocIConvertor convertor = new DocIConvertor();
            Source source = convertor.convert(result, UUID.randomUUID().toString(), input.toURI().toString());
            path = new File(PathUtils.formatPath(String.format("%s/%s-source.json",
                    dir.getAbsolutePath(), name)));
            try (FileOutputStream os = new FileOutputStream(path)) {
                String json = JSONUtils.asString(source);
                os.write(json.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
        } catch (Throwable t) {
            DefaultLogger.stacktrace(t);
            fail(t);
        }
    }
}