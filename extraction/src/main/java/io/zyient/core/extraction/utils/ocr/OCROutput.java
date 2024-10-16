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

import io.zyient.core.extraction.model.Source;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class OCROutput {
    private String sourcePath;
    private String outputDir;
    private OCRFileType outputType;
    private String outputPdf;
    private Map<Integer, String> ocrOutputs;

    public void addOcrOutput(int page, String path) {
        if (ocrOutputs == null) {
            ocrOutputs = new HashMap<>();
        }
        ocrOutputs.put(page, path);
    }

    public Source as() throws Exception {
        return null;
    }
}
