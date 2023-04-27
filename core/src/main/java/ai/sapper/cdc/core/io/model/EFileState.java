package ai.sapper.cdc.core.io.model;

public enum EFileState {
    Unknown,
    New,
    PendingSync,
    Synced,
    Deleted,
    Error
}
