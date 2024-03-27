package io.zyient.core.mapping.pipeline;

import io.zyient.core.mapping.model.SourceInputContentInfo;
import io.zyient.core.mapping.readers.ReadResponse;
import lombok.NonNull;

public interface PipelineInMemory {
    ReadResponse read( @NonNull SourceInputContentInfo context) throws Exception;
}
