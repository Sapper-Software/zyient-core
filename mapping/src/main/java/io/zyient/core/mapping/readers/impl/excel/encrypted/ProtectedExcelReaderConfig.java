package io.zyient.core.mapping.readers.impl.excel.encrypted;

import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.impl.excel.ExcelReaderConfig;
import io.zyient.core.mapping.readers.settings.ProtectedExcelReaderSettings;
import lombok.NonNull;

public class ProtectedExcelReaderConfig extends ExcelReaderConfig {
    public ProtectedExcelReaderConfig() {
        super(ProtectedExcelReaderSettings.class);
    }

    @Override
    public InputReader createInstance(@NonNull InputContentInfo contentInfo) throws Exception {
        return (new ProtectedExcelInputReader()).contentInfo(contentInfo).settings(this.settings());
    }
}
