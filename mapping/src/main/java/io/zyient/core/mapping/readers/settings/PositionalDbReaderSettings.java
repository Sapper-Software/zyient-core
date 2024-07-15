package io.zyient.core.mapping.readers.settings;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.zyient.core.mapping.model.Column;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class PositionalDbReaderSettings extends DbReaderSettings {
    private Map<Integer, Column> columns;
}
