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

package ai.sapper.cdc.common.utils;

public class CompareUtils {
    public static int compare(double v1, double v2) {
        double r = v1 - v2;
        if (r < 1 && r > 0) {
            return 1;
        } else if (r > -1 && r < 0) {
            return -1;
        }
        return (int) r;
    }
    public static int compare(float v1, float v2) {
        float r = v1 - v2;
        if (r < 1 && r > 0) {
            return 1;
        } else if (r > -1 && r < 0) {
            return -1;
        }
        return (int) r;
    }

}
