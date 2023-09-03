package ai.sapper.cdc.common.threads;

public interface IThreadListener {
    void event(EThreadEvent event, Thread thread, Object... params);
}
