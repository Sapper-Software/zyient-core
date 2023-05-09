package ai.sapper.cdc.core.io.impl.local;

import ai.sapper.cdc.common.config.Config;
import ai.sapper.cdc.core.io.model.Container;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.IOException;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class LocalContainer extends Container {
    @Config(name = "path")
    private String path;
}
