package ai.sapper.cdc.entity;

public enum EEntityState {
    UNKNOWN,
    SNAPSHOT,
    ACTIVE,
    ACTIVE_PENDING,
    DELETED,
    PAUSED,
    ERROR,
    PAUSED_SCHEMA_CHANGE,
    PAUSED_SCHEMA_ERROR
}
