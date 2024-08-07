package io.zyient.core.mapping.readers.impl.db.positional;

import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.common.model.entity.IKey;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.core.mapping.model.PositionalColumn;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.readers.impl.db.DbInputReader;
import io.zyient.core.mapping.readers.settings.PositionalDbReaderSettings;
import io.zyient.core.mapping.readers.settings.PositionalReaderSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PositionalDBInputReader<K extends IKey, E extends IEntity<K>> extends DbInputReader<K, E> {
    @Override
    public List<SourceMap> fetchNextBatch() throws IOException {
        try {
            List<E> data = new ArrayList<>();
            while (data.size() < settings().getReadBatchSize() && !cache.isEmpty()) {
                E entity = cache.poll();
                data.add(entity);
            }
            if (!data.isEmpty()) {
                List<SourceMap> values = new ArrayList<>(data.size());
                for (E entity : data) {
                    Map<String, Object> map = JSONUtils.asMap(entity);
                    String line = (String) ((Map<String, Object>) map.get("properties")).get("VAL1");
                    map.put("properties", parse(line));
                    values.add(new SourceMap(map));
                }
                return values;
            }
            return null;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private SourceMap parse(String line) throws Exception {
        PositionalDbReaderSettings settings = (PositionalDbReaderSettings) settings();
        SourceMap map = new SourceMap();
        for (int ii = 0; ii < settings.getColumns().size(); ii++) {
            PositionalColumn column = (PositionalColumn) settings.getColumns().get(ii);
            int end = -1;
            if (column.getLength() != null) {
                end = column.getPosStart() + column.getLength();
            } else {
                end = column.getPosEnd();
            }
            if (line.length() <= end) {
                end = line.length() - 1;
            }
            String value = "";
            if (column.getPosStart() < line.length()) {
                value = line.substring(column.getPosStart(), end);
            }
            if (map.get(column.getName()) == null) {
                map.put(column.getName(), value.trim());
            } else {
                map.put(column.getName(), value.trim().concat((String) map.get(column.getName())));
            }
        }
        return map;
    }


}
