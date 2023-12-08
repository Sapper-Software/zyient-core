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

package io.zyient.core.mapping.transformers;

import io.zyient.base.common.utils.DefaultLogger;
import org.junit.jupiter.api.Test;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class NumericTransformerTest {

    @Test
    void clean() {
        try {
            NumberFormat usf = NumberFormat.getInstance(Locale.US);
            Number number = usf.parse("120,009,999.7888");
            assertEquals(120009999.7888, number.doubleValue());
            DecimalFormat df = (DecimalFormat) DecimalFormat.getInstance(Locale.US);
            number = df.parse("12,345.00$");
            assertEquals(12345.00f, number.doubleValue());
            Locale l = new Locale("tr", "TR");
            NumberFormat ff = NumberFormat.getInstance(l);
            number = ff.parse("120.009.999,7888");
            // assertEquals(120009999.7888, number.doubleValue());
            String v = ff.format(120009999.7888);
            System.out.println(v);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }
}