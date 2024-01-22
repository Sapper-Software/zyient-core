/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.core.extraction.view;

import com.google.common.base.Strings;
import io.zyient.base.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.ResourceBundle;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

class ViewManagerTest {

    @Test
    void getDisplayText() {
        try {
            DefaultLogger.info("Current Locale: " + Locale.getDefault());
            ResourceBundle bundle = ResourceBundle.getBundle("test_resource");
            String value = bundle.getString("KEY1");
            assertFalse(Strings.isNullOrEmpty(value));

            Locale.setDefault(new Locale("ms", "MY"));
            bundle = ResourceBundle.getBundle("test_resource");
            value = bundle.getString("KEY2");
            assertFalse(Strings.isNullOrEmpty(value));

        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}