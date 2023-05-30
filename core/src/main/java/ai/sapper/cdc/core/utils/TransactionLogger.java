package ai.sapper.cdc.core.utils;

import ai.sapper.cdc.common.utils.DefaultLogger;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionLogger extends DefaultLogger {
    private static final Logger LOG = LoggerFactory.getLogger("ai.sapper");

    public void debug(@NonNull Class<?> caller, Object txId, String mesg) {
        LOG.debug(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void info(@NonNull Class<?> caller, Object txId, String mesg) {
        LOG.info(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void warn(@NonNull Class<?> caller, Object txId, String mesg) {
        LOG.warn(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void error(@NonNull Class<?> caller, Object txId, String mesg) {
        LOG.error(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), mesg));
    }

    public void error(Class<?> caller, Object txId, Throwable t) {
        LOG.error(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), t.getLocalizedMessage()));
        if (LOG.isDebugEnabled()) {
            DefaultLogger.stacktrace(t);
        }
    }

    public void error(@NonNull Class<?> caller, Object txId, String mesg, Throwable t) {
        LOG.error(String.format("[TXID=%s][%s]: %s", txId, caller.getCanonicalName(), mesg), t);
        if (LOG.isDebugEnabled()) {
            DefaultLogger.stacktrace(t);
        }
    }

    public static final TransactionLogger LOGGER = new TransactionLogger();
}
