package io.zyient.core.mapping.mapper;

import com.unifiedframework.model.block.CaseDocument;

public class UDPEntityMapping extends JPathMapping<CaseDocument> {
    protected UDPEntityMapping() {
        super(CaseDocument.class, UDPMappedResponse.class);
    }
}
