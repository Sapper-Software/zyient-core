package io.zyient.core.mapping.mapper;

import io.zyient.core.mapping.model.Holding;

public class HoldingEntityMapping extends Mapping<Holding> {
    protected HoldingEntityMapping() {
        super(Holding.class, HoldingMappedResponse.class);
    }
}
