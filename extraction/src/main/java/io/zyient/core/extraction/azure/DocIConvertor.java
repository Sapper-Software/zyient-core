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

import com.azure.ai.formrecognizer.documentanalysis.models.*;
import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.core.extraction.ExtractionConvertor;
import io.zyient.core.extraction.model.*;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.*;

public class DocIConvertor implements ExtractionConvertor<AnalyzeResult> {

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class StyleIndexElement {
        private int start;
        private int end;
        private String textColor;
        private String backgroundColor;
        private FontInfo fontInfo;
        private double confidence;

        public int length() {
            return end - start;
        }
    }

    public static class StyleIndex {
        private final Map<Integer, Map<Integer, StyleIndexElement>> index = new HashMap<>();

        public StyleIndex add(@NonNull StyleIndexElement style) {
            Map<Integer, StyleIndexElement> node = index.computeIfAbsent(style.start, k -> new HashMap<>());
            node.put(style.end, style);
            return this;
        }

        public StyleIndexElement get(int start, int end) {
            if (index.containsKey(start)) {
                Map<Integer, StyleIndexElement> node = index.get(start);
                return node.get(end);
            }
            return null;
        }

        public Map<Integer, StyleIndexElement> get(int start) {
            return index.get(start);
        }

        public StyleIndexElement find(int start, int end) {
            for (int s : index.keySet()) {
                if (s >= end) continue;
                Map<Integer, StyleIndexElement> map = index.get(s);
                StyleIndexElement se = null;
                for (int e : map.keySet()) {
                    if (e >= start) {
                        StyleIndexElement el = map.get(end);
                        if (se == null) {
                            se = el;
                        } else if (el.length() > se.length()) {
                            se = el;
                        }
                    }
                }
                return se;
            }
            return null;
        }
    }

    @Getter
    @Setter
    @Accessors(fluent = true)
    public static class LanguageElement {
        private int start;
        private int end;
        private String name;
        private Locale locale;
        private double confidence;


        public int length() {
            return end - start;
        }
    }

    public static class LanguageIndex {
        private final Map<Integer, Map<Integer, LanguageElement>> locales = new HashMap<>();

        public LanguageIndex add(@NonNull LanguageElement language) {
            Map<Integer, LanguageElement> node = locales.computeIfAbsent(language.start, k -> new HashMap<>());
            node.put(language.end, language);
            return this;
        }

        public LanguageElement get(int start, int end) {
            if (locales.containsKey(start)) {
                Map<Integer, LanguageElement> node = locales.get(start);
                return node.get(end);
            }
            return null;
        }

        public Map<Integer, LanguageElement> get(int start) {
            return locales.get(start);
        }

        public LanguageElement find(int start, int end) {
            for (int s : locales.keySet()) {
                if (s >= end) continue;
                Map<Integer, LanguageElement> map = locales.get(s);
                LanguageElement se = null;
                for (int e : map.keySet()) {
                    if (e >= start) {
                        LanguageElement el = map.get(end);
                        if (se == null) {
                            se = el;
                        } else if (el.length() > se.length()) {
                            se = el;
                        }
                    }
                }
                return se;
            }
            return null;
        }

        public boolean isEmpty() {
            return locales.isEmpty();
        }

        public Set<Integer> keySet() {
            return locales.keySet();
        }
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
        StyleIndex styleIndex = createStyleIndex(source);
        LanguageIndex languageIndex = createLanguageIndex(source);
        Map<Integer, DocIPageParser> parsers = new HashMap<>();
        try {
            int count = 0;
            if (source.getPages() != null) {
                for (int ii = 0; ii < source.getPages().size(); ii++) {
                    DocumentPage docPage = source.getPages().get(ii);
                    DocIPageParser parser = new DocIPageParser(source,
                            doc, docPage, styleIndex, languageIndex,
                            ii + 1);
                    parsers.put(ii + 1, parser);
                    parser.parse();
                    count++;
                }
            }
            if (source.getTables() != null) {
                for (DocumentTable table : source.getTables()) {
                    processTable(table, parsers, doc, count);
                    count++;
                }
            }
        } finally {
            for (DocIPageParser parser : parsers.values()) {
                parser.close();
            }
        }
        return output;
    }

    private void processTable(DocumentTable table,
                              Map<Integer, DocIPageParser> parsers,
                              DocumentSection section,
                              int index) throws Exception {
        Table tab = section.add(Table.class, index);
        Map<Integer, Section> sections = new HashMap<>();
        for (BoundingRegion region : table.getBoundingRegions()) {
            if (sections.containsKey(region.getPageNumber())) continue;
            Section s = tab.newSection(region.getPageNumber());
            if (region.getBoundingPolygon() == null || region.getBoundingPolygon().isEmpty()) {
                Page page = section.findPage(region.getPageNumber());
                if (page == null) {
                    throw new Exception(String.format("Page not found. [index=%s]", region.getPageNumber()));
                }
                s.setBoundingBox(page.getBoundingBox());
            } else {
                BoundingBox bb = DocIPageParser.createBoundingBox(region.getBoundingPolygon());
                s.setBoundingBox(bb);
            }
            sections.put(region.getPageNumber(), s);
        }
        List<DocumentTableCell> cells = table.getCells();
        if (cells != null && !cells.isEmpty()) {
            for (DocumentTableCell cell : cells) {
                if (cell.getKind().equals(DocumentTableCellKind.COLUMN_HEADER)) {
                    Column column = tab.addHeader(cell.getContent(), cell.getColumnIndex());
                    DefaultLogger.debug(String.format("[table=%s] Added column: [name=%s][index=%d]",
                            tab.getId(), column.getData(), column.getIndex()));
                }
                if (cell.getBoundingRegions().size() > 1) {

                }
            }
        }
    }

    private LanguageIndex createLanguageIndex(AnalyzeResult source) throws Exception {
        List<DocumentLanguage> languages = source.getLanguages();
        if (languages != null && !languages.isEmpty()) {
            LanguageIndex index = new LanguageIndex();
            for (DocumentLanguage language : languages) {
                List<DocumentSpan> spans = language.getSpans();
                if (spans != null && !spans.isEmpty()) {
                    Locale locale = new Locale(language.getLocale());
                    for (DocumentSpan span : spans) {
                        LanguageElement le = new LanguageElement();
                        le.start = span.getOffset();
                        le.end = le.start + span.getLength();
                        le.name = language.getLocale();
                        le.locale = locale;
                        le.confidence = language.getConfidence();
                        index.add(le);
                    }
                }
            }
            return index;
        }
        return null;
    }

    private StyleIndex createStyleIndex(AnalyzeResult source) throws Exception {
        List<DocumentStyle> styles = source.getStyles();
        if (styles != null && !styles.isEmpty()) {
            StyleIndex index = new StyleIndex();
            for (DocumentStyle style : styles) {
                List<DocumentSpan> spans = style.getSpans();
                if (spans != null && !spans.isEmpty()) {
                    FontInfo fi = getFontInfo(style);
                    for (DocumentSpan span : spans) {
                        StyleIndexElement se = new StyleIndexElement();
                        se.start = span.getOffset();
                        se.end = se.start + span.getLength();
                        se.textColor = style.getColor();
                        se.backgroundColor = style.getBackgroundColor();
                        se.confidence = style.getConfidence();
                        se.fontInfo = fi;
                        index.add(se);
                    }
                }
            }
            return index;
        }
        return null;
    }

    private FontInfo getFontInfo(DocumentStyle style) {
        if (style.isHandwritten() != null && style.isHandwritten()) {
            return FontInfo.HANDWRITTEN;
        }
        FontInfo fi = new FontInfo();
        if (!Strings.isNullOrEmpty(style.getSimilarFontFamily())) {
            fi.setName(style.getSimilarFontFamily());
        }
        if (style.getFontStyle() != null) {
            if (style.getFontStyle().equals(FontStyle.ITALIC)) {
                fi.setItalics(true);
            }
        }
        if (style.getFontWeight() != null) {
            if (style.getFontWeight().equals(FontWeight.BOLD)) {
                fi.setBold(true);
            }
        }
        return fi;
    }

    public static SizeUnit parseSizeUnit(DocumentPageLengthUnit unit) {
        if (unit.equals(DocumentPageLengthUnit.INCH)) {
            return SizeUnit.Inches;
        } else if (unit.equals(DocumentPageLengthUnit.PIXEL)) {
            return SizeUnit.Pixel;
        }
        return null;
    }

}
