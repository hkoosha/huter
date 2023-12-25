package io.koosha.huter.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

public abstract class CloseableManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CloseableManager.class);

    private final Collection<AutoCloseable> closeables = new ArrayList<>();

    protected CloseableManager() {
    }

    @Override
    public final void close() throws Exception {
        LOG.trace("do close: {}", getClass().getName());
        Throwable err = null;
        for (final AutoCloseable closeable : this.closeables)
            try {
                closeable.close();
            }
            catch (final Throwable e) {
                LOG.error("error closing closable in={} closable={}", getClass().getName(), closeable, e);
                err = HuterThrowables.merge(err, e);
            }

        closeables.clear();

        if (err instanceof Exception)
            throw ((Exception) err);
        else if (err != null)
            throw new Exception(err);
    }

    public final void addClosable(final AutoCloseable closable) {
        // Prevent nasty cycles.
        if (closable instanceof CloseableManager)
            throw new IllegalArgumentException("DEADBEEF");

        this.closeables.add(closable);
    }

}
