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

import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisClient;
import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisClientBuilder;
import com.azure.ai.formrecognizer.documentanalysis.models.AnalyzeResult;
import com.azure.ai.formrecognizer.documentanalysis.models.OperationResult;
import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.BinaryData;
import com.azure.core.util.polling.SyncPoller;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.core.extraction.Extractor;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class DocIExtractor implements Extractor<AnalyzeResult> {
    private final ProcessorState state = new ProcessorState();
    private DocISettings settings;
    private DocumentAnalysisClient client;

    @Override
    public Extractor<AnalyzeResult> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                              @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, Extractor.__CONFIG_PATH, DocISettings.class);
            reader.read();
            settings = (DocISettings) reader.settings();
            String key = env.keyStore().read(settings().getKey());
            if (Strings.isNullOrEmpty(key)) {
                throw new Exception(String.format("Key not found. [name=%s]", settings.getKey()));
            }
            client = new DocumentAnalysisClientBuilder()
                    .credential(new AzureKeyCredential(key))
                    .endpoint(settings().getEndpoint())
                    .buildClient();
            state.setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state.error(ex);
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    public AnalyzeResult extract(@NonNull File source, Context context) throws Exception {
        state.check(ProcessorState.EProcessorState.Running);
        if (!source.exists()) {
            throw new IOException(String.format("Source not found. [path=%s]", source.getAbsolutePath()));
        }
        try (FileInputStream fis = new FileInputStream(source)) {
            BinaryData data = BinaryData.fromStream(fis, source.length());
            SyncPoller<OperationResult, AnalyzeResult> poller =
                    client.beginAnalyzeDocument(settings().getModelId(), data);

            AnalyzeResult result = poller.getFinalResult();
            if (DefaultLogger.isTraceEnabled()) {
                DefaultLogger.trace(source.getAbsolutePath(), result);
            }
            return result;
        }
    }

    @Override
    public void close() throws IOException {
        if (state.hasError()) {
            state.setState(ProcessorState.EProcessorState.Stopped);
        }
        if (client != null) {
            client = null;
        }
    }
}
