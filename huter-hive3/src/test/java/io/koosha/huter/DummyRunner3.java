package io.koosha.huter;

import io.koosha.huter.internal.HuterFiles;
import io.koosha.huter.main.HuterMain;

import java.nio.file.Paths;

/**
 * See {@link io.koosha.huter.main.HuterRepoMain}.
 */
public final class DummyRunner3 {
    private DummyRunner3() {
    }

    private static final String TARGET = "/tmp/huter/test";

    private static final String[] ARGS = {
            "--root=" + TARGET,
            "--log-dir=" + TARGET + "/log",
            "-q SELECT TRUE",
            "-t SELECT TRUE"
    };

    public static void main(final String... args) {
        try {
            HuterFiles.deleteDir(Paths.get(TARGET));
        }
        catch (final Exception ignore) {
        }

        HuterMain.main(ARGS.clone());
    }

}
