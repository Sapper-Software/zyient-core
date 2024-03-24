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

package io.zyient.core.extraction.utils;

import com.google.common.base.Preconditions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.KeyValuePair;
import io.zyient.core.extraction.model.Cell;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.opencv.core.Mat;
import org.opencv.core.MatOfPoint;
import org.opencv.core.Point;
import org.opencv.core.Scalar;
import org.opencv.imgproc.Imgproc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@Accessors(fluent = true)
public class CellDetector {
    private final Scalar fillColor = new Scalar(0, 255, 0);
    private Mat image;
    private Mat greyscale;

    public void decorate(@NonNull Cell<?> cell) throws Exception {
        Preconditions.checkNotNull(image);
        Point topLeft = new Point(cell.getBoundingBox().getStart().getX(), cell.getBoundingBox().getStart().getY());
        Point bottomRight = new Point(cell.getBoundingBox().getEnd().getX(), cell.getBoundingBox().getEnd().getY());
        int width = (int) Math.abs(bottomRight.x - topLeft.x) + 1;
        int height = (int) Math.abs(topLeft.y - bottomRight.y) + 1;
        int x1 = (int) topLeft.x;
        int x2 = (int) bottomRight.x;
        if (x2 < x1) {
            int t = x2;
            x2 = x1;
            x1 = t;
        }
        int y1 = (int) topLeft.y;
        int y2 = (int) bottomRight.y;
        if (y2 < y1) {
            int t = y2;
            y2 = y1;
            y1 = t;
        }
        int index = 0;
        Map<Scalar, Integer> counts = new HashMap<>();
        for (int ii = x1; ii <= x2; ii++) {
            for (int jj = y1; jj <= y2; jj++) {
                double[] d = image.get(jj, ii);
                if (d == null) {
                    DefaultLogger.debug(String.format("Point is null. [x=%d][y=%d]", ii, jj));
                }
                Scalar c = new Scalar(d);
                if (!counts.containsKey(c)) {
                    counts.put(c, 1);
                } else {
                    int cc = counts.get(c);
                    counts.put(c, cc + 1);
                }
                index++;
            }
        }
        List<MatOfPoint> poly = new ArrayList<>(4);
        MatOfPoint p = new MatOfPoint(topLeft, new Point(topLeft.x, bottomRight.y), bottomRight, new Point(bottomRight.x, topLeft.y), topLeft);
        poly.add(p);
        if (greyscale != null) {
            Imgproc.fillPoly(greyscale, poly, fillColor);
        } else {
            Imgproc.fillPoly(image, poly, fillColor);
        }

        KeyValuePair<Scalar, Integer> bg = new KeyValuePair<>();
        KeyValuePair<Scalar, Integer> tc = new KeyValuePair<>();
        for (Scalar sc : counts.keySet()) {
            int count = counts.get(sc);
            if (bg.key() == null) {
                bg.key(sc);
                bg.value(count);
                continue;
            } else {
                if (count > bg.value()) {
                    tc.key(bg.key());
                    tc.value(bg.value());
                    bg.key(sc);
                    bg.value(count);
                    continue;
                }
            }
            if (tc.key() == null) {
                tc.key(sc);
                tc.value(count);
            } else if (count > tc.value()) {
                tc.key(sc);
                tc.value(count);
            }
        }
        cell.setBackground(bg.key());
        // cell.setTextColor(tc.key());
    }
}
