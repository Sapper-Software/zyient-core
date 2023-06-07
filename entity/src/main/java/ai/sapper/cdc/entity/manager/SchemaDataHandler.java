package ai.sapper.cdc.entity.manager;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.entity.schema.Domain;
import ai.sapper.cdc.entity.schema.EntitySchema;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import ai.sapper.cdc.entity.schema.SchemaVersion;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.Closeable;
import java.util.List;

@Getter
@Accessors(fluent = true)
public abstract class SchemaDataHandler implements Closeable {
    private final Class<? extends SchemaDataHandlerSettings> settingsType;
    private BaseEnv<?> env;
    protected Connection connection;
    private SchemaDataHandlerSettings settings;

    protected SchemaDataHandler(@NonNull Class<? extends SchemaDataHandlerSettings> settingsType) {
        this.settingsType = settingsType;
    }

    public SchemaDataHandler init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                  @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, settingsType);
            reader.read();
            return init((SchemaDataHandlerSettings) reader.settings(), env);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public SchemaDataHandler init(@NonNull SchemaDataHandlerSettings settings,
                                  @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            this.env = env;
            this.settings = settings;
            init(settings);
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }


    protected String schemaCacheKey(SchemaEntity entity) {
        return String.format("%s::%s", entity.getDomain(), entity.getEntity());
    }

    protected String schemaCacheKey(SchemaEntity entity, SchemaVersion version) {
        return String.format("%s::%s::%d.%d",
                entity.getDomain(), entity.getEntity(),
                version.getMajorVersion(), version.getMinorVersion());
    }

    public abstract void init(@NonNull SchemaDataHandlerSettings settings) throws Exception;


    protected abstract Domain saveDomain(@NonNull Domain domain) throws Exception;

    protected abstract Domain fetchDomain(@NonNull String domain) throws Exception;

    protected abstract boolean deleteDomain(@NonNull Domain domain) throws Exception;

    protected abstract SchemaEntity fetchEntity(@NonNull String domain, @NonNull String entity) throws Exception;

    protected abstract SchemaEntity saveEntity(@NonNull SchemaEntity entity) throws Exception;

    protected abstract boolean deleteEntity(@NonNull SchemaEntity entity) throws Exception;

    protected abstract EntitySchema fetchSchema(@NonNull SchemaEntity entity,
                                                SchemaVersion version) throws Exception;

    protected abstract EntitySchema fetchSchema(@NonNull SchemaEntity entity,
                                                @NonNull String uri) throws Exception;

    protected abstract EntitySchema saveSchema(@NonNull EntitySchema schema) throws Exception;

    protected abstract boolean deleteSchema(@NonNull SchemaEntity entity,
                                            @NonNull SchemaVersion version) throws Exception;

    protected abstract List<Domain> listDomains() throws Exception;

    protected abstract String schemaCacheKey(@NonNull SchemaEntity entity,
                                             @NonNull String uri) throws Exception;
}
