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

package io.zyient.base.core.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Strings;
import io.zyient.base.common.GlobalConstants;
import jakarta.ws.rs.core.MediaType;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.io.FilenameUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class FileTypeDetector {
    public static final String[] COMMON_SEPARATORS = {",", "\t", "\\|", ";", ":"};
    public static final int MATCH_THRESHOLD = 20;

    private final File file;
    private SourceTypes type = SourceTypes.UNKNOWN;
    private String separator = null;

    public FileTypeDetector(File file) {
        this.file = file;
    }

    public SourceTypes detect() throws Exception {
        String ext = FilenameUtils.getExtension(file.getAbsolutePath());
        type = SourceTypes.fromExtension(ext);
        if (type != SourceTypes.UNKNOWN) return type;
        String mimeType = FileUtils.detectMimeType(file);
        type = from(mimeType);
        if (type != SourceTypes.UNKNOWN) return type;
        if (FileUtils.MIME_TYPE_TEXT.compareToIgnoreCase(mimeType) == 0) {
            checkTextFile();
        }
        return type;
    }

    private void checkTextFile() throws Exception {
        if (!file.exists()) return;

        checkSeparatedFile();
        if (type != SourceTypes.UNKNOWN) return;
        checkJson();
        if (type != SourceTypes.UNKNOWN) return;
        checkXml();
    }

    private void checkXml() throws Exception {
        XmlMapper mapper = new XmlMapper();
        try {
            Map<?, ?> map = mapper.readValue(file, Map.class);
            type = SourceTypes.XML;
        } catch (Exception ex) {
            // Do nothing...
        }
    }

    private void checkJson() throws Exception {
        ObjectMapper mapper = GlobalConstants.getJsonMapper();
        try {
            Map<?, ?> map = mapper.readValue(file, Map.class);
            type = SourceTypes.JSON;
        } catch (Exception ex) {
            // Check JSON Array
            try {
                Map<?, ?>[] array = mapper.readValue(file, Map[].class);
                type = SourceTypes.JSON;
            } catch (Exception nex) {
                // Not a JSON
            }
        }
    }

    private void checkSeparatedFile() throws Exception {
        int currentSplitterIndex = -1;
        for (currentSplitterIndex = 0; currentSplitterIndex < COMMON_SEPARATORS.length; currentSplitterIndex++) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                int columnCount = -1;
                int matchCount = 0;
                int matchFailed = 0;
                while (true) {
                    String line = reader.readLine();
                    if (line == null) {
                        if (matchFailed == 0 || matchCount / matchFailed > 1) {
                            separator = COMMON_SEPARATORS[currentSplitterIndex];
                        }
                        break;
                    }
                    if (Strings.isNullOrEmpty(line)) continue;
                    String[] parts = line.split(COMMON_SEPARATORS[currentSplitterIndex]);
                    if (parts.length <= 1) break;
                    if (columnCount == -1) {
                        columnCount = parts.length;
                    } else if (columnCount == parts.length) {
                        matchCount++;
                    } else {
                        matchFailed++;
                    }
                    if (matchCount >= MATCH_THRESHOLD) {
                        separator = COMMON_SEPARATORS[currentSplitterIndex];
                        break;
                    }
                }
            }
            if (separator != null) break;
        }
        if (!Strings.isNullOrEmpty(separator)) {
            switch (currentSplitterIndex) {
                case 0:
                case 3:
                case 4:
                    type = SourceTypes.CSV;
                    break;
                case 1:
                    type = SourceTypes.TSV;
                    break;
                case 2:
                    type = SourceTypes.PSV;
                    break;
            }
        }
    }

    public static SourceTypes from(@NonNull String mimeType) {
        if (FileUtils.isXmlType(mimeType)) {
            return SourceTypes.XML;
        } else if (FileUtils.isExcelType(mimeType)) {
            return SourceTypes.EXCEL;
        } else if (FileUtils.MIME_TYPE_CSV.compareToIgnoreCase(mimeType) == 0) {
            return SourceTypes.CSV;
        } else if (FileUtils.MIME_TYPE_HTML.compareToIgnoreCase(mimeType) == 0) {
            return SourceTypes.HTML;
        } else if (FileUtils.MIME_TYPE_JSON.compareToIgnoreCase(mimeType) == 0) {
            return SourceTypes.JSON;
        } else if (FileUtils.isArchiveType(mimeType)) {
            return SourceTypes.COMPRESSED;
        }
        return SourceTypes.UNKNOWN;
    }

    public static MediaType as(@NonNull SourceTypes type) {
        switch (type) {
            case CSV, EXCEL_CSV, PSV, RFC4180, TSV, POSITIONAL -> {
                return MediaType.TEXT_PLAIN_TYPE;
            }
            case PDF, EXCEL, PPT, WORD, COMPRESSED -> {
                return MediaType.APPLICATION_OCTET_STREAM_TYPE;
            }
            case XML -> {
                return MediaType.APPLICATION_XML_TYPE;
            }
            case JSON -> {
                return MediaType.APPLICATION_JSON_TYPE;
            }
            case HTML -> {
                return MediaType.APPLICATION_XHTML_XML_TYPE;
            }
        }
        return MediaType.WILDCARD_TYPE;
    }
}
