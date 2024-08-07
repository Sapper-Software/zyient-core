package io.zyient.core.mapping.readers.impl.separated;

import com.google.common.base.Preconditions;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.settings.FlattenedInputReaderSettings;
import lombok.NonNull;

public class FlattenedReaderConfig extends SeparatedReaderConfig{
    public FlattenedReaderConfig() {
        super( FlattenedInputReaderSettings.class);
    }

    @Override
    public InputReader createInstance(@NonNull InputContentInfo contentInfo) throws Exception {
        Preconditions.checkState(settings() instanceof FlattenedInputReaderSettings);
        return new FlattenedInputReader()
                .contentInfo(contentInfo)
                .settings(settings());
    }
}
