package io.zyient.core.mapping.model;

import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.readers.ReadCompleteCallback;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.List;

@Getter
@Setter
@Accessors(fluent = true)
public class SourceInputContentInfo extends InputContentInfo {

    private ReadCompleteCallback callback;
    private List<SourceMap> sourceMaps;

    public SourceInputContentInfo(List<SourceMap> sourceMaps) {
        this.sourceMaps = sourceMaps;
    }

}
