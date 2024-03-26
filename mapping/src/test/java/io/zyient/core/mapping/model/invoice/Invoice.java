package io.zyient.core.mapping.model.invoice;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class Invoice {

    private InvoiceHeader header;
    private List<InvoiceLineItem> items;
}
