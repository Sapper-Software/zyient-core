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

package io.zyient.base.core.utils;

import io.zyient.base.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.fail;

class FileUtilsTest {
    private static final String CSV = "src/test/resources/data/business-financial-data-june-2023-quarter-csv.csv";
    private static final String EXCEL = "src/test/resources/data/Financial Sample.xlsx";
    private static final String PSV = "src/test/resources/data/psv-sample.psv";
    private static final String JSON = "src/test/resources/data/test-json.json";

    @Test
    void detectMimeType() {
        try {
            String mime = FileUtils.detectMimeType(new File(CSV));
            mime = FileUtils.detectMimeType(new File(EXCEL));
            mime = FileUtils.detectMimeType(new File(PSV));
            mime = FileUtils.detectMimeType(new File(JSON));
            System.out.println("Done...");
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}