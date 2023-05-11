package ai.sapper.cdc.core.io.model;

public enum EFileState {
    Unknown,
    New,
    Updating,
    PendingSync,
    Synced,
    Deleted,
    Error,
    UploadFailed
}
