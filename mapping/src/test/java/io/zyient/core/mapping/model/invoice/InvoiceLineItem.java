package io.zyient.core.mapping.model.invoice;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class InvoiceLineItem {
    private String orderLine;
    private Double quantityShipped;
    private Double quantityOrdered;
    private Double total;
    private Double unitPrice;
}
