package ai.sapper.cdc.entity.executor;

public class FatalError extends RuntimeException {
    private static final String __PREFIX = "FATAL ERROR: %s";

    public FatalError(String message) {
        super(String.format(__PREFIX, message));
    }

    public FatalError(String message, Throwable cause) {
        super(String.format(__PREFIX, message), cause);
    }

    public FatalError(Throwable cause) {
        super(String.format(__PREFIX, cause.getLocalizedMessage()), cause);
    }
}
