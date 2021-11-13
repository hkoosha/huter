package com.trivago.huter;

import com.trivago.huter.main.HuterMain;
import com.trivago.huter.util.HuterUtil;

import java.nio.file.Paths;

public final class DummyRunner {
    private DummyRunner() {
    }

    private static final String TARGET = "/tmp/huter";

    private static final String[] ARGS = {
        "--root=" + TARGET,
        "--log-dir=" + TARGET + "/log",
        "-q SELECT FALSE",
        "-t SELECT FALSE"
    };

    public static void main(final String... args) {
        try {
            HuterUtil.deleteDir(Paths.get(TARGET));
        }
        catch (final Exception ignore) {
        }

        HuterMain.main(ARGS.clone());
    }

}
