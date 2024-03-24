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

import ch.ethz.globis.phtree.PhTreeF;
import com.azure.ai.formrecognizer.documentanalysis.models.*;
import io.zyient.core.extraction.model.Point;
import io.zyient.core.extraction.model.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class DocIPageParser implements Closeable {
    private static class MarkedNode<T> {
        private T node;
        private boolean used = false;

        public MarkedNode(@NonNull T node) {
            this.node = node;
        }
    }

    private static class WordNode {
        private final String id;
        private final List<MarkedNode<TextCell>> nodes = new ArrayList<>();

        public WordNode(@NonNull BoundingBox bb) {
            id = parseId(bb.getStart().getX(), bb.getStart().getY());
        }
    }

    private static class LineNode {
        private final String id;
        private final List<MarkedNode<TextLine>> nodes = new ArrayList<>();

        public LineNode(@NonNull BoundingBox bb) {
            id = parseId(bb.getStart().getX(), bb.getStart().getY());
        }
    }

    public static String parseId(double X, double Y) {
        return String.format("%f::%f", X, Y);
    }

    private final AnalyzeResult source;
    private final DocumentSection section;
    private final DocumentPage docPage;
    private final int index;
    private final DocIConvertor.StyleIndex styleIndex;
    private final DocIConvertor.LanguageIndex languageIndex;

    private PhTreeF<double[]> wordMap;
    private PhTreeF<double[]> lineMap;

    private final Map<String, WordNode> wordIndex = new HashMap<>();
    private final Map<String, LineNode> lineIndex = new HashMap<>();

    public DocIPageParser(@NonNull AnalyzeResult source,
                          @NonNull DocumentSection section,
                          @NonNull DocumentPage docPage,
                          DocIConvertor.StyleIndex styleIndex,
                          DocIConvertor.LanguageIndex languageIndex,
                          int index) {
        this.source = source;
        this.section = section;
        this.docPage = docPage;
        this.styleIndex = styleIndex;
        this.languageIndex = languageIndex;
        this.index = index;
    }

    public Page parse() throws Exception {
        Page page = section.add(index);
        BoundingBox bb = new BoundingBox();
        bb.setPage(index);
        bb.add(0, 0)
                .add(new Point(docPage.getWidth(), docPage.getHeight()));
        page.setSizeUnit(DocIConvertor.parseSizeUnit(docPage.getUnit()));
        if (docPage.getHeight() != null) {
            page.setHeight(docPage.getHeight());
        }
        if (docPage.getWidth() != null) {
            page.setWidth(docPage.getWidth());
        }
        List<DocumentWord> words = docPage.getWords();
        if (words != null && !words.isEmpty()) {
            wordMap = PhTreeF.create(2);
            int count = 0;
            for (DocumentWord word : words) {
                processWord(page, word, count);
                count++;
            }
        }
        List<DocumentLine> lines = docPage.getLines();
        if (lines != null && !lines.isEmpty()) {
            lineMap = PhTreeF.create(2);
            int count = 0;
            for (DocumentLine line : lines) {
                processLine(page, line, count);
                count++;
            }
        }
        if (source.getTables() != null && !source.getTables().isEmpty()) {
            for (DocumentTable table : source.getTables()) {
                List<BoundingRegion> brs = table.getBoundingRegions();
                int count = 0;
                for (BoundingRegion br : brs) {
                    if (br.getPageNumber() == index) {
                        processTable(page, table, count);
                        count++;
                    }
                }
            }
        }
        return page;
    }

    private void processTable(Page page, DocumentTable table, int index) {
        Table tab = new Table(page.getParentId(), index);
    }

    private void processLine(Page page, DocumentLine line, int index) throws Exception {
        TextLine cell = new TextLine(page.getParentId(), index);
        BoundingBox bb = createBoundingBox(line.getBoundingPolygon());
        cell.setBoundingBox(bb);
        cell.setData(line.getContent());
        PhTreeF.PhQueryF<double[]> results = wordMap.query(new double[]{bb.getStart().getX(), bb.getStart().getY()},
                new double[]{bb.getEnd().getX(), bb.getEnd().getY()});
        if (results != null) {
            while (results.hasNext()) {
                double[] rec = results.next();
                String id = parseId(rec[0], rec[1]);
                if (wordIndex.containsKey(id)) {
                    WordNode node = wordIndex.get(id);
                    for (MarkedNode<TextCell> tc : node.nodes) {
                        TextCell c = tc.node;
                        if (c.getBoundingBox().getEnd().getX() <= bb.getEnd().getX() &&
                                c.getBoundingBox().getEnd().getY() <= bb.getEnd().getY()) {
                            tc.used = true;
                            cell.add(c);
                        }
                    }
                }
            }
        }
        lineMap.put(new double[]{bb.getStart().getX(), bb.getStart().getY()},
                new double[]{bb.getStart().getX(), bb.getStart().getY()});
        String id = parseId(bb.getStart().getX(), bb.getStart().getY());
        LineNode node = lineIndex.get(id);
        if (node == null) {
            node = new LineNode(bb);
            lineIndex.put(id, node);
        }
        node.nodes.add(new MarkedNode<>(cell));
    }

    private void processWord(Page page, DocumentWord word, int index) throws Exception {
        TextCell cell = new TextCell(page.getParentId(), index);
        BoundingBox bb = createBoundingBox(word.getBoundingPolygon());
        cell.setBoundingBox(bb);
        cell.setData(word.getContent());
        DocumentSpan span = word.getSpan();
        int spanEnd = span.getOffset() + span.getLength();
        cell.setSpan(new CellSpan(span.getOffset(), span.getLength()));
        if (styleIndex != null) {
            DocIConvertor.StyleIndexElement se = styleIndex.get(span.getOffset(), spanEnd);
            if (se == null) {
                se = styleIndex.find(span.getOffset(), spanEnd);
            }
            if (se != null) {
                cell.setFontInfo(se.fontInfo());
                cell.setTextColor(se.textColor());
                cell.setBackgroundColor(se.backgroundColor());
            }
        }
        if (languageIndex != null) {
            DocIConvertor.LanguageElement le = languageIndex.get(span.getOffset(), spanEnd);
            if (le == null) {
                languageIndex.find(span.getOffset(), spanEnd);
            }
            if (le != null) {
                if (le.locale() != null) {
                    cell.setLocale(le.locale().getLanguage());
                } else {
                    cell.setLocale(le.name());
                }
            }
        }
        wordMap.put(new double[]{bb.getStart().getX(), bb.getStart().getY()},
                new double[]{bb.getStart().getX(), bb.getStart().getY()});
        String id = parseId(bb.getStart().getX(), bb.getStart().getY());
        WordNode node = wordIndex.get(id);
        if (node == null) {
            node = new WordNode(cell.getBoundingBox());
            wordIndex.put(node.id, node);
        }
        node.nodes.add(new MarkedNode<>(cell));
    }

    private BoundingBox createBoundingBox(List<com.azure.ai.formrecognizer.documentanalysis.models.Point> points) {
        BoundingBox bb = new BoundingBox();
        for (com.azure.ai.formrecognizer.documentanalysis.models.Point point : points) {
            bb.add(point.getX(), point.getY());
        }
        return bb;
    }

    @Override
    public void close() throws IOException {
        if (wordMap != null) {
            wordMap.clear();
            wordMap = null;
        }
    }
}
