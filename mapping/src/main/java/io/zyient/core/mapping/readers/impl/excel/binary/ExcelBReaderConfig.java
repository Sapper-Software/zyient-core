package io.zyient.core.mapping.readers.impl.excel.binary;

import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.impl.excel.ExcelInputReader;
import io.zyient.core.mapping.readers.impl.excel.ExcelReaderConfig;
import lombok.NonNull;

public class ExcelBReaderConfig extends ExcelReaderConfig {


    @Override
    public InputReader createInstance(@NonNull InputContentInfo contentInfo) throws Exception {
        return (new ExcelInputReader()).contentInfo(contentInfo).settings(this.settings());
    }
}
