package io.zyient.core.mapping.readers.impl.context;

import com.google.common.base.Preconditions;
import io.zyient.base.core.utils.SourceTypes;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.InputReaderConfig;
import io.zyient.core.mapping.readers.settings.ContextReaderSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;


public class ContextReaderConfig extends InputReaderConfig {
    public ContextReaderConfig() {
        super(new SourceTypes[]{SourceTypes.CONTEXT}, ContextReaderSettings.class);
    }

    @Override
    protected void configureReader(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws Exception {

    }

    @Override
    public InputReader createInstance(@NonNull InputContentInfo contentInfo) throws Exception {
        Preconditions.checkState(settings() instanceof ContextReaderSettings);
        return new ContextInputReader()
                .contentInfo(contentInfo)
                .settings(settings());
    }
}
