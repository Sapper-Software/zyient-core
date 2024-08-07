package io.zyient.core.mapping.mapper;

import io.zyient.base.common.model.Context;
import io.zyient.core.mapping.model.mapping.MappedElement;
import io.zyient.core.mapping.model.mapping.MappingType;
import io.zyient.core.mapping.transformers.Transformer;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Map;

public interface IMapTransformer<T> {
    @Getter
    @Setter
    @Accessors(fluent = true)
    class MapNode {
        protected String name;
        protected Class<?> type;
        protected String targetPath;
        protected Map<String, MapNode> nodes;
        protected Transformer<?> transformer;
        protected boolean nullable;
        protected MappingType mappingType;
        protected Object reference;
    }
    IMapTransformer<T> add(@NonNull MappedElement element) throws Exception;

    Object transform(@NonNull MappedElement element, @NonNull Object source) throws Exception;

    Map<String, Object> transform(@NonNull Map<String, Object> source,
                                  @NonNull Class<? extends T> entityType, @NonNull Context context) throws Exception;
}
