package io.zyient.core.mapping.readers.impl.separated.encrypted;

import com.google.common.base.Preconditions;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.impl.separated.SeparatedReaderConfig;
import io.zyient.core.mapping.readers.settings.EncryptedSeparatedReaderSettings;
import lombok.NonNull;

public class EncryptedSeparatedReaderConfig extends SeparatedReaderConfig {
    public EncryptedSeparatedReaderConfig() {
        super(EncryptedSeparatedReaderSettings.class);
    }


    @Override
    public InputReader createInstance(@NonNull InputContentInfo contentInfo) throws Exception {
        Preconditions.checkState(settings() instanceof EncryptedSeparatedReaderSettings);
        return new EncryptedSeparatedInputReader()
                .contentInfo(contentInfo)
                .settings(settings());
    }
}
