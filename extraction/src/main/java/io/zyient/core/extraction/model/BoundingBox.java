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
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class BoundingBox {
    private int page;
    private Point start;
    private Point end;
    private List<Point> points;

    public BoundingBox add(@NonNull Point point) {
        return add(point.getX(), point.getY());
    }

    public BoundingBox add(double x, double y) {
        Point point = new Point(x, y);
        if (points == null) {
            points = new ArrayList<>();
        }
        points.add(point);
        if (start == null) {
            start = point;
        } else {
            if (start.getX() > point.getX()) {
                start.setX(point.getX());
            }
            if (start.getY() > point.getY()) {
                start.setY(point.getY());
            }
            if (end == null) {
                end = point;
            } else {
                if (end.getX() < point.getX()) {
                    end.setX(point.getX());
                }
                if (end.getY() < point.getY()) {
                    end.setY(point.getY());
                }
            }
        }
        return this;
    }
}
