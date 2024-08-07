package io.zyient.core.mapping.mapper.db2;

import io.zyient.base.common.model.Context;
import io.zyient.base.core.BaseEnv;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

public interface IDbMapperBuilder {

    String __CONFIG_PATH = "dbMapper";

    IDbMapperBuilder configure(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                                  @NonNull BaseEnv<?> env) throws ConfigurationException;

    DBMapper build(Context context) throws Exception;

}
