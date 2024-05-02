package io.zyient.core.mapping.pipeline;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.ValidationException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.processing.ProcessorState;
import io.zyient.base.core.utils.Timer;
import io.zyient.core.mapping.mapper.JPathMapping;
import io.zyient.core.mapping.mapper.MapperFactory;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.model.RecordResponse;
import io.zyient.core.mapping.model.mapping.MappedResponse;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.pipeline.settings.ExecutablePipelineSettings;
import io.zyient.core.mapping.readers.InputReader;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.ReadResponse;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;

@Getter
@Setter
@Accessors(fluent = true)
public class StatelessPipeline<E> extends ExecutablePipeline<E> implements PipelineSource {
    @Override
    public String name() {
        Preconditions.checkNotNull(mapping());
        return mapping().name();
    }

    @Override
    public StatelessPipeline<E> configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                          @NonNull MapperFactory mapperFactory,
                                          @NonNull BaseEnv<?> env) throws ConfigurationException {
        try {
            withMapperFactory(mapperFactory);
            super.configure(xmlConfig, env, ExecutablePipelineSettings.class);
            state().setState(ProcessorState.EProcessorState.Running);
            return this;
        } catch (Exception ex) {
            state().error(ex);
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    @Override
    protected RecordResponse execute(@NonNull SourceMap data, Context context) throws Exception {

        RecordResponse response = new RecordResponse();
        MappedResponse<E> r = mapping().read(data, context);
        response.setStatus(r.getStatus());
        response.setEntity(r.getEntity());
        return response;
    }

    public ReadResponse read(@NonNull InputReader reader, @NonNull InputContentInfo context) throws Exception {
        ReadResponse response = new ReadResponse();
        ReadCursor cursor = reader.open(env());
        while (true) {
            try (Timer t = new Timer(metrics.processTimer())) {
                RecordResponse r = new RecordResponse();
                try {
                    SourceMap data = cursor.next();
                    if (data == null) break;
                    metrics.recordsCounter().increment();
                    r.setSource(data);
                    response.incrementCount();
                    r = process(data, context);
                    response.add(r);
                    response.incrementCommitCount();
                    metrics.processedCounter().increment();
                } catch (ValidationException | ValidationExceptions ex) {
                    String msg = String.format("[file=%s][record=%d] Validation Failed: %s",
                            reader.input().getAbsolutePath(), response.getRecordCount(), ex.getLocalizedMessage());
                    ValidationExceptions ve = ValidationExceptions.add(new ValidationException(msg), null);
                    if (settings().isTerminateOnValidationError()) {
                        DefaultLogger.stacktrace(ex);
                        throw ve;
                    } else {
                        metrics.errorsCounter().increment();
                        response.incrementCount();
                        DefaultLogger.warn(msg);
                        r = errorResponse(r, null, ex);
                        response.add(r);
                    }
                } catch (Exception e) {
                    DefaultLogger.stacktrace(e);
                    DefaultLogger.error(e.getLocalizedMessage());
                    throw e;
                }
            }
        }
        return response;
    }

}
