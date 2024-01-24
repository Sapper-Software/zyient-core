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

package io.zyient.core.extraction.utils.ocr;

import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.core.extraction.model.LanguageCode;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class OCRTest {
    private static final String __INPUT = "src/test/resources/input/table-view-01.png";
    private static final String __INPUT_PDF = "src/test/resources/input/sec-filing-sample-01.pdf";
    private static final String __INPUT_CHINESE_01 = "src/test/resources/input/chinese-invoice-sample-01.png";
    private static final String __DATA_PATH = "/usr/share/tesseract-ocr/4.00/tessdata";

    @Test
    void run() {
        try {
            File ourDir = PathUtils.getTempDir();
            DefaultLogger.info(String.format("Using output directory : [%s]", ourDir.getAbsolutePath()));
            File input = new File(__INPUT_PDF);
            TesseractOCR ocr = new TesseractOCR(OCRFileType.PDF, input.getAbsolutePath(), ourDir.getAbsolutePath(), __DATA_PATH);
            ocr.outputType(OCRFileType.Alto)
                    .detectLanguage(false)
                    .show(false)
                    .language(LanguageCode.ENGLISH);
            ocr.run();
            DefaultLogger.info("Finished....");
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}