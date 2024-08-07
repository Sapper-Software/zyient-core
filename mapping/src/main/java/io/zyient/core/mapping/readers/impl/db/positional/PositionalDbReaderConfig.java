package io.zyient.core.mapping.readers.impl.db.positional;

import com.google.common.base.Preconditions;
import io.zyient.core.mapping.model.Column;
import io.zyient.core.mapping.model.PositionalColumn;
import io.zyient.core.mapping.readers.impl.db.DbReaderConfig;
import io.zyient.core.mapping.readers.settings.PositionalDbReaderSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.util.Map;

public class PositionalDbReaderConfig extends DbReaderConfig {
    public PositionalDbReaderConfig() {
        super(PositionalDbReaderSettings.class);
    }

    @Override
    protected void configureReader(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        super.configureReader(xmlConfig);

        Preconditions.checkState(settings() instanceof PositionalDbReaderSettings);
        Map<Integer, Column> columns = Column.read(xmlConfig, PositionalColumn.class);
        if (columns == null || columns.isEmpty()) {
            throw new Exception("Positional Columns not specified...");
        }
        for (int ii = 0; ii < columns.size(); ii++) {
            if (!columns.containsKey(ii)) {
                throw new Exception(String.format("Missing column index. [index=%d]", ii));
            }
        }
        ((PositionalDbReaderSettings) settings()).setColumns(columns);
    }


}
