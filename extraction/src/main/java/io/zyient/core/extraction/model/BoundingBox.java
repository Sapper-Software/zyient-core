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
public class BoundingBox {
    private int page;
    private Point start;
    private Point end;

    public BoundingBox start(double x, double y) {
        start = new Point();
        start.setX(x);
        start.setY(y);

        return this;
    }

    public BoundingBox end(double x, double y) {
        end = new Point();
        end.setX(x);
        end.setY(y);

        return this;
    }
}
