package ai.sapper.cdc.common.model;

import lombok.NonNull;

public class InvalidDataError extends Exception {
    private static final String __PREFIX = "Invalid Data: %s [type=%s]";

    public InvalidDataError(@NonNull Class<?> type, String message) {
        super(String.format(__PREFIX, message, type.getCanonicalName()));
    }

    public InvalidDataError(@NonNull Class<?> type, String message, Throwable cause) {
        super(String.format(__PREFIX, message, type.getCanonicalName()), cause);
    }

    public InvalidDataError(@NonNull Class<?> type, Throwable cause) {
        super(String.format(__PREFIX, cause.getLocalizedMessage(), type.getCanonicalName()), cause);
    }
}
