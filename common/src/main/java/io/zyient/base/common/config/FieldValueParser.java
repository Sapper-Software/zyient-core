package io.zyient.base.common.config;

import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public interface FieldValueParser<T> {
    T parse(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException;

    class DummyParser implements FieldValueParser<Object> {

        @Override
        public Object parse(@NonNull HierarchicalConfiguration<ImmutableNode> config) throws ConfigurationException {
            throw new ConfigurationException("Should not be called...");
        }
    }
}
