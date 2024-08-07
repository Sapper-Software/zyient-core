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

package io.zyient.core.extraction.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FontInfo {
    private String name;
    private double size;
    private boolean bold = false;
    private boolean underlined = false;
    private boolean italics = false;

    public FontInfo() {

    }

    public FontInfo(String name, double size, boolean bold, boolean underlined, boolean italics) {
        this.name = name;
        this.size = size;
        this.bold = bold;
        this.underlined = underlined;
        this.italics = italics;
    }

    public static final String FONT_NAME_HANDWRITTEN = "Hand Writing";

    public static final FontInfo HANDWRITTEN = new FontInfo(FONT_NAME_HANDWRITTEN, 0, false, false, false);
}
