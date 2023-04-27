package ai.sapper.cdc.common.config;

import lombok.NonNull;


public interface ConfigValueParser<T> {
    T parse(@NonNull String value) throws Exception;

    String serialize(@NonNull T value) throws Exception;

    class DummyValueParser implements ConfigValueParser<Object> {

        @Override
        public Object parse(@NonNull String value) throws Exception {
            return null;
        }

        @Override
        public String serialize(@NonNull Object value) throws Exception {
            return null;
        }
    }
}
