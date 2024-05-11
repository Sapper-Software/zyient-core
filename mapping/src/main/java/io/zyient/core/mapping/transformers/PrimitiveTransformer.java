package io.zyient.core.mapping.transformers;

public interface PrimitiveTransformer<T> {
    Class<T> getPrimitiveType();

    T getDefaultPrimitiveValue();
}
