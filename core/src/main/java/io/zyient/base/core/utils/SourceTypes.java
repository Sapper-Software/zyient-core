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

import lombok.Getter;
import lombok.NonNull;

@Getter
public enum SourceTypes {
    UNKNOWN("unknown", null),
    CSV("text/csv", new String[]{"csv","dat"}),
    TSV("text/tsv", new String[]{"tsv", "tdf"}),
    RFC4180("text/rfc4180", null),
    PSV("text/psv", new String[]{"psv","pgp","txt"}),
    EXCEL_CSV("excel/csv", null),
    XML(FileUtils.MIME_TYPE_XML, new String[]{"xml"}),
    JSON(FileUtils.MIME_TYPE_JSON, new String[]{"json"}),
    POSITIONAL("text/pos", new String[]{"pos","x380"}),
    EXCEL("application/ms-excel", new String[]{"xls", "xlsx","xlsb"}),
    HTML(FileUtils.MIME_TYPE_HTML, new String[]{"html", "dhtml"}),
    PDF(FileUtils.MIME_TYPE_PDF, new String[]{"pdf"}),
    WORD("application/ms-word", new String[]{"doc", "docx"}),
    PPT("application/ms-powerpoint", new String[]{"ppt"}),
    COMPRESSED(FileUtils.MIME_TYPE_ZIP, new String[]{"zip", "bzip", "bzip2", "tar", "gz", "7z", "rar"}),
    DB("application/data", new String[]{}),
    CONTEXT("application/data",new String[]{});


    private final String mimeType;
    private final String[] extensions;

    SourceTypes(@NonNull String mimeType, String[] extensions) {
        this.mimeType = mimeType;
        this.extensions = extensions;
    }

    public boolean matches(@NonNull String ext) {
        if (extensions != null) {
            for (String extension : extensions) {
                if (extension.compareToIgnoreCase(ext) == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    public static SourceTypes fromExtension(@NonNull String ext) {
        for (SourceTypes t : SourceTypes.values()) {
            if (t.matches(ext)) {
                return t;
            }
        }
        return UNKNOWN;
    }

    public static SourceTypes parse(@NonNull String mimeType) {
        for (SourceTypes t : SourceTypes.values()) {
            if (t.mimeType.compareToIgnoreCase(mimeType) == 0) {
                return t;
            }
        }
        return UNKNOWN;
    }

    public static SourceTypes from(@NonNull String type) {
        for (SourceTypes t : SourceTypes.values()) {
            if (t.name().compareToIgnoreCase(type) == 0) {
                return t;
            }
        }
        return null;
    }
}
