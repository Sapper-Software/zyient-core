package io.zyient.core.mapping.mapper;

import com.unifiedframework.model.block.CaseDocument;
import io.zyient.core.mapping.annotations.EntityRef;
import io.zyient.core.mapping.model.mapping.MappedResponse;

import java.util.Map;
@EntityRef(type = CaseDocument.class)
public class UDPMappedResponse extends MappedResponse<CaseDocument> {
    public UDPMappedResponse(Map<String, Object> source) {
        super(source);
    }
}

