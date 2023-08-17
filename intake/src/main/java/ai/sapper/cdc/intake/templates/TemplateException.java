package ai.sapper.cdc.intake.templates;

public class TemplateException extends Exception {
    private static final String PREFIX = "Template Error : %s";

    /**
     * Exception constructor with error message string.
     *
     * @param s - Error message string.
     */
    public TemplateException(String s) {
        super(String.format(PREFIX, s));
    }

    /**
     * Exception constructor with error message string and inner cause.
     *
     * @param s         - Error message string.
     * @param throwable - Inner cause.
     */
    public TemplateException(String s, Throwable throwable) {
        super(String.format(PREFIX, s), throwable);
    }

    /**
     * Exception constructor inner cause.
     *
     * @param throwable - Inner cause.
     */
    public TemplateException(Throwable throwable) {
        super(String.format(PREFIX, throwable.getLocalizedMessage()), throwable);
    }
}
