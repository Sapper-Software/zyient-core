package io.zyient.core.mapping.mapper;

import io.zyient.core.mapping.model.invoice.Invoice;

public class InvoiceEntityMapping extends JPathMapping<Invoice> {
    protected InvoiceEntityMapping() {
        super(Invoice.class, InvoiceMappedResponse.class);
    }
}
