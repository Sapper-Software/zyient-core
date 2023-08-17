package ai.sapper.cdc.intake.model;

public enum EContentSource {
    /**
     * Template Read from local file.
     */
    FILE,
    /**
     * Template Read from Resources
     */
    RESOURCE,
    /**
     * Template Downloaded from S3
     */
    S3
}
