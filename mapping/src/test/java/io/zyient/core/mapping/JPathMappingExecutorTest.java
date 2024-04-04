package io.zyient.core.mapping;

import com.google.common.base.Preconditions;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.model.services.EConfigFileType;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.RunUtils;
import io.zyient.base.core.utils.SourceTypes;
import io.zyient.core.mapping.env.DemoDataStoreEnv;
import io.zyient.core.mapping.model.InputContentInfo;
import io.zyient.core.mapping.model.mapping.SourceMap;
import io.zyient.core.mapping.readers.ReadCompleteCallback;
import io.zyient.core.mapping.readers.ReadCursor;
import io.zyient.core.mapping.readers.ReadResponse;
import io.zyient.core.mapping.readers.impl.json.JsonInputReader;
import io.zyient.core.mapping.readers.settings.JsonReaderSettings;
import lombok.NonNull;
import org.apache.commons.configuration2.XMLConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.fail;

public class JPathMappingExecutorTest {
    private static final String __CONFIG_FILE = "src/test/resources/mapping/test-mapping-env.xml";
    private static final String __CONFIG_FILE_MAPPING = "src/test/resources/mapping/invoice-mapping.xml";
    private static final String __INPUT_CUSTOMER_CSV = "src/test/resources/data/invoice_udp.json";

    private static XMLConfiguration xmlConfiguration = null;
    private static DemoDataStoreEnv env = new DemoDataStoreEnv();

    @BeforeAll
    static void beforeAll() throws Exception {
        xmlConfiguration = ConfigReader.read(__CONFIG_FILE, EConfigFileType.File);
        Preconditions.checkState(xmlConfiguration != null);
        env.create(xmlConfiguration);
        env.connectionManager().save();

        XMLConfiguration mConfig = ConfigReader.readFromFile(__CONFIG_FILE_MAPPING);
        MappingExecutor.create(mConfig, env);
    }

    @AfterAll
    static void afterAll() throws Exception {
        env.close();
    }

    @Test
    void read() {
        try {
            File input = new File(__INPUT_CUSTOMER_CSV);
            InputContentInfo ci = new InputContentInfo();
            Callback callback = new Callback();
            ci.path(input)
                    .sourceURI(input.toURI())
                    .documentId(UUID.randomUUID().toString());

            JsonInputReader reader = new JsonInputReader();
            reader.contentInfo(ci);
            JsonReaderSettings jsonReaderSettings = new JsonReaderSettings();
            jsonReaderSettings.setArray(false);
            jsonReaderSettings.setAssumeType(SourceTypes.JSON);
            jsonReaderSettings.setName("json-reader");
            jsonReaderSettings.setReadBatchSize(100);
            reader.settings(jsonReaderSettings);
            ReadCursor readCursor = reader.doOpen();
            List<SourceMap> sourceMaps = new ArrayList<>();
            while (true) {
                List<SourceMap> s = readCursor.reader().fetchNextBatch();
                if (s == null) break;
                sourceMaps.addAll(s);
            }
            InputContentInfo info = new InputContentInfo();
            info.put("country", "India");
            info.put("mappingName", "demo");
            info.put("filterId", "1");
            info.put("udp_json",sourceMaps);
            info.callback(callback);
            info.contentType(SourceTypes.CONTEXT);
            MappingExecutor.defaultInstance().read(info);
            while (!callback.finished) {
                RunUtils.sleep(1000);
            }
            if (callback.error != null) {
                fail(callback.error);
            }
            RunUtils.sleep(1000);
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            fail(ex);
        }
    }

    public static class Callback implements ReadCompleteCallback {
        private boolean finished = false;
        private Throwable error;

        @Override
        public void onError(@NonNull InputContentInfo contentInfo, @NonNull Throwable error) {
            DefaultLogger.error("Read pipeline failed.", error);
            this.error = error;
            finished = true;
        }

        @Override
        public void onSuccess(@NonNull InputContentInfo contentInfo, @NonNull ReadResponse response) {
            try {
                String jc = JSONUtils.asString(response);
                DefaultLogger.info(String.format("INPUT=[%s]", jc));
                finished = true;
            } catch (Exception ex) {
                DefaultLogger.stacktrace(ex);
                error = ex;
                finished = true;
            }
        }
    }
}
