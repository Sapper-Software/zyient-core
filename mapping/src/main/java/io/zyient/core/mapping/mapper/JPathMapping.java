package io.zyient.core.mapping.mapper;

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.decisions.builder.EvaluationTreeBuilder;
import io.zyient.core.mapping.mapper.db2.DBMapper;
import io.zyient.core.mapping.mapper.db2.DbMapperBuilder;
import io.zyient.core.mapping.mapper.db2.DbMapperSettings;
import io.zyient.core.mapping.model.mapping.Mapped;
import io.zyient.core.mapping.model.mapping.MappedElement;
import io.zyient.core.mapping.model.mapping.MappedResponse;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import javax.naming.ConfigurationException;
import java.util.Map;

public abstract class JPathMapping<T> extends Mapping<T> {

    private Context context;
    private DbMapperBuilder builder;

    public void withContext(Context context) {
        this.context = context;
    }

    protected JPathMapping(@NonNull Class<? extends T> entityType, @NonNull Class<? extends MappedResponse<T>> responseType) {
        super(entityType, responseType);
    }

    public IMapTransformer<T> buildTransformer(HierarchicalConfiguration<ImmutableNode> mNode, String configPath) throws Exception {

        if (ConfigReader.checkIfNodeExists(mNode, DbMapperBuilder.__CONFIG_PATH)) {
            HierarchicalConfiguration<ImmutableNode> bNode = mNode.configurationAt(DbMapperBuilder.__CONFIG_PATH);
            Class<? extends DbMapperBuilder> type = (Class<? extends DbMapperBuilder>) ConfigReader.readType(bNode);
            if (type == null) {
                throw new Exception("Evaluation Tree builder type not specified...");
            }
            builder = (DbMapperBuilder) type.getDeclaredConstructor().newInstance().configure(bNode, env);

        }
        return null;
    }

    public void initializeIfRequired() throws Exception {
        if (mapTransformer() == null) {
            DBMapper mapper = builder.build(context);
            IMapTransformer<T> transformer = new JPathMapTransformer<>(entityType, settings(), mapper);
            int i = 0;
            for (DBMapper.DBConfNode dbConfNode : mapper.getNodes()) {
                try {
                    MappedElement m = dbConfNode.getConf().as();
                    transformer.add(m);
                    sourceIndex.put(i, m);
                    i++;
                } catch (Exception e) {
                    DefaultLogger.stacktrace(e);
                    throw new ConfigurationException(e.getMessage());
                }
            }
            withTransformer(transformer);
        }

    }

}
