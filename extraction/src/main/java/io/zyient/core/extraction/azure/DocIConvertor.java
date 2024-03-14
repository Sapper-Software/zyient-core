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
import com.azure.ai.formrecognizer.documentanalysis.models.DocumentPageLengthUnit;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.extraction.ExtractionConvertor;
import io.zyient.core.extraction.model.*;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DocIConvertor implements ExtractionConvertor<AnalyzeResult> {
    private static class Word {
        private int index;
        private String text;
        private BoundingBox box;
        private boolean used = false;
    }

    private static class WordIndex {
        private final List<Word> words = new ArrayList<>();
        private final Map<String, List<Word>> index = new HashMap<>();

    }

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
            convertPage(doc, docPage, ii);
        }
        return output;
    }

    private void convertPage(DocumentSection section,
                             DocumentPage docPage,
                             int index) throws Exception {
        Page page = section.add(index);
        BoundingBox bb = new BoundingBox();
        bb.setPage(index);
        bb.start(0, 0);
        bb.setEnd(new Point(docPage.getWidth(), docPage.getHeight()));
        page.setSizeUnit(parseSizeUnit(docPage.getUnit()));
        if (docPage.getHeight() != null) {
            page.setHeight(docPage.getHeight());
        }
        if (docPage.getWidth() != null) {
            page.setWidth(docPage.getWidth());
        }
        for (int ii = 0; ii < docPage.getLines().size(); ii++) {
            DocumentLine line = docPage.getLines().get(ii);

        }
    }

    private SizeUnit parseSizeUnit(DocumentPageLengthUnit unit) {
        if (unit.equals(DocumentPageLengthUnit.INCH)) {
            return SizeUnit.Inches;
        } else if (unit.equals(DocumentPageLengthUnit.PIXEL)) {
            return SizeUnit.Pixel;
        }
        return null;
    }

    private BoundingBox findBoundingBox(List<com.azure.ai.formrecognizer.documentanalysis.models.Point> points) {
        BoundingBox bb = new BoundingBox();
        float minX = Float.MAX_VALUE;
        float minY = Float.MAX_VALUE;
        float maxX = Float.MIN_VALUE;
        float maxY = Float.MIN_VALUE;

        for (com.azure.ai.formrecognizer.documentanalysis.models.Point point : points) {
            if (point.getX() < minX) {
                minX = point.getX();
            } else if (point.getX() > maxX) {
                maxX = point.getX();
            }
            if (point.getY() < minY) {
                minY = point.getY();
            } else if (point.getY() > maxY) {
                maxY = point.getY();
            }
        }
        bb.setStart(new Point(minX, minY));
        bb.setEnd(new Point(maxX, maxY));
        return bb;
    }
}
