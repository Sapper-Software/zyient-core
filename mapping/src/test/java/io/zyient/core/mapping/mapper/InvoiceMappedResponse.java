package io.zyient.core.mapping.mapper;

import io.zyient.core.mapping.annotations.EntityRef;
import io.zyient.core.mapping.model.invoice.Invoice;
import io.zyient.core.mapping.model.mapping.MappedResponse;

import java.util.Map;

@EntityRef(type = Invoice.class)
public class InvoiceMappedResponse extends MappedResponse<Invoice> {
    public InvoiceMappedResponse(Map<String, Object> source) {
        super(source);
    }
}



