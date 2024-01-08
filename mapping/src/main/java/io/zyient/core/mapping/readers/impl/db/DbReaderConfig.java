package io.zyient.core.mapping.readers.impl.db;

import com.google.common.base.Preconditions;
import io.zyient.base.core.utils.SourceTypes;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.InputReaderConfig;
import io.zyient.core.mapping.readers.settings.DbReaderSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public class DbReaderConfig extends InputReaderConfig {
    private Class<? extends DbInputReader<?, ?>> readerType;

    public DbReaderConfig() {
        super(new SourceTypes[]{SourceTypes.DB}, DbReaderSettings.class);
    }

    @Override
    protected void configureReader(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {
        Preconditions.checkState(settings() instanceof DbReaderSettings);
        readerType = ((DbReaderSettings) settings()).getReaderType();
    }

    @Override
    public InputReader createInstance(@NonNull InputContentInfo contentInfo) throws Exception {
        return readerType.getDeclaredConstructor()
                .newInstance()
                .contentInfo(contentInfo)
                .settings(settings());
    }
}
