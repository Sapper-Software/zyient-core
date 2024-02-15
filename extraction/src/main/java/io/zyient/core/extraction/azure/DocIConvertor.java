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
import com.azure.ai.formrecognizer.documentanalysis.models.DocumentLine;
import com.azure.ai.formrecognizer.documentanalysis.models.DocumentPage;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.extraction.ExtractionConvertor;
import io.zyient.core.extraction.model.*;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class DocIConvertor implements ExtractionConvertor<AnalyzeResult> {
    @Override
    public ExtractionConvertor<AnalyzeResult> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                                        @NonNull BaseEnv<?> env) throws ConfigurationException {
        return this;
    }

    @Override
    public Source convert(@NonNull AnalyzeResult source,
                          @NonNull String sourceReferenceId,
                          @NonNull String sourceUri) throws Exception {
        Source output = new Source(sourceReferenceId, sourceUri);
        DocumentSection doc = output.create(0);
        for (int ii = 0; ii < source.getPages().size(); ii++) {
            DocumentPage docPage = source.getPages().get(ii);
            processPage(doc, docPage, ii);
        }
        return output;
    }

    private void processPage(DocumentSection section,
                             DocumentPage docPage,
                             int index) throws Exception {
        Page page = section.add(index);
        BoundingBox bb = new BoundingBox();
        bb.setPage(index);
        bb.start(0, 0);
        bb.setEnd(new Point(docPage.getWidth(), docPage.getHeight()));
        for (int ii = 0; ii < docPage.getLines().size(); ii++) {
            DocumentLine line = docPage.getLines().get(ii);

        }
    }
}
