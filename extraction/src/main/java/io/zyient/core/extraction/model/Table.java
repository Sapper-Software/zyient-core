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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class Table extends Section {
    public static final String __PREFIX = "TB.";

    private Map<String, Column> header;

    public Table() {
        super();
    }

    public Table(@NonNull String parentId, int index) {
        super(parentId, index);
    }

    @Override
    protected Cell<?> find(String parentId, @NonNull String[] paths, int index) {
        if (checkId(parentId, paths, index)) {
            if (index == paths.length - 1) {
                return this;
            } else {
                String name = paths[index + 1];
                if (name.startsWith(Column.__PREFIX)) {
                    if (header != null) {
                        for (String key : header.keySet()) {
                            Column column = header.get(key);
                            Cell<?> ret = column.find(getId(), paths, index + 1);
                            if (ret != null) {
                                return ret;
                            }
                        }
                    }
                } else {
                    if (getBlocks() != null) {
                        for (Cell<?> row : getBlocks()) {
                            Cell<?> ret = row.find(getId(), paths, index + 1);
                            if (ret != null) {
                                return ret;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    protected String parseId(int index) {
        return String.format("%s%d", __PREFIX, index);
    }

    public Column addHeader(@NonNull String name, int index) {
        if (header == null) {
            header = new HashMap<>();
        }
        Column column = new Column(getId(), index);
        column.setData(name);
        header.put(name, column);
        return column;
    }

    public Section newSection(int pageNo) throws Exception {
        Preconditions.checkArgument(pageNo > 0);
        return add(Section.class, pageNo);
    }

    public Row addRow(int section, int index) throws Exception {
        Preconditions.checkArgument(section > 0);
        Preconditions.checkArgument(index >= 0);
        Section s = (Section) getBlocks().get(section);
        Row row = new Row(this, index);
        s.add(row, index);
        return row;
    }

    public void addCell(int section, int row,
                        @NonNull String column,
                        @NonNull Cell<?> cell) throws Exception {
        Preconditions.checkArgument(section > 0);
        Preconditions.checkArgument(row >= 0);
        Section s = (Section) getBlocks().get(section);
        if (s == null) {
            throw new Exception(String.format("[table=%s] Section not found. [section=%d]",
                    getId(), section));
        }
        Column c = header.get(column);
        if (c == null) {
            throw new Exception(String.format("[table=%s] Column not found. [name=%s]",
                    getId(), column));
        }
        Row r = (Row) s.getBlocks().get(row);
        r.add(cell, c.getIndex());
    }

    public void addCell(int section, int row, int column,
                        @NonNull Cell<?> cell) throws Exception {
        Preconditions.checkArgument(section > 0);
        Preconditions.checkArgument(row >= 0);
        Preconditions.checkArgument(column >= 0);
        Section s = (Section) getBlocks().get(section);
        if (s == null) {
            throw new Exception(String.format("[table=%s] Section not found. [section=%d]",
                    getId(), section));
        }
        Row r = (Row) s.getBlocks().get(row);
        r.add(cell, column);
    }
}
