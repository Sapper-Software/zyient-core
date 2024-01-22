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

import io.zyient.base.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class FileUtilsTest {
    private static final String CSV = "src/test/resources/data/business-financial-data-june-2023-quarter-csv.csv";
    private static final String EXCEL = "src/test/resources/data/Financial Sample.xlsx";
    private static final String PSV = "src/test/resources/data/psv-sample.pdv";
    private static final String JSON = "src/test/resources/data/test-json.j";
    private static final String XML = "src/test/resources/data/sample.xmm";

    @Test
    void detectMimeType() {
        try {
            FileTypeDetector detector = new FileTypeDetector(new File(CSV));
            detector.detect();
            assertEquals(SourceTypes.CSV, detector.type());
            detector = new FileTypeDetector(new File(EXCEL));
            detector.detect();
            assertEquals(SourceTypes.EXCEL, detector.type());
            detector = new FileTypeDetector(new File(PSV));
            detector.detect();
            assertEquals(SourceTypes.PSV, detector.type());
            detector = new FileTypeDetector(new File(JSON));
            detector.detect();
            assertEquals(SourceTypes.JSON, detector.type());
            detector = new FileTypeDetector(new File(XML));
            detector.detect();
            assertEquals(SourceTypes.XML, detector.type());
            System.out.println("Done...");
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}