package io.koosha.huter.main;

import io.koosha.huter.internal.HuterFiles;
import io.koosha.huter.runner.RepoRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static io.koosha.huter.internal.HuterCollections.freezer;

public final class HuterRepoMain {

    private static final Logger LOG = LoggerFactory.getLogger(HuterRepoMain.class);

    private HuterRepoMain() {
        throw new UnsupportedOperationException("utility class can not be instantiated.");
    }


    public static void main(final String... args) throws Exception {
        Thread.currentThread().setName(HuterRepoMain.class.getSimpleName());

        final List<String> err = run(args);

        if (err.isEmpty()) {
            LOG.info("all ok");
            System.exit(0);
        }
        else {
            LOG.error("errors: {}", err);
            System.exit(1);
        }
    }

    public static List<String> run(final String... args) throws Exception {
        if (args.length != 1)
            throw new IllegalArgumentException("expecting only one argument referring to repo directory, got: " + args.length);

        final String repoDir = args[0];
        if (!HuterFiles.isDir(repoDir))
            throw new IOException("given path does not exist or is not a directory: " + repoDir);

        final List<Object[]> run;
        try (final RepoRunner hr = new RepoRunner(repoDir)) {
            run = hr.run();
        }

        // Repo runner returns single list of list-of-errors.
        if (run.size() != 1)
            throw new IllegalStateException("expecting only one output, got: " + run.size());

        final Object[] err = run.get(0);
        return Arrays.stream(err)
                     .map(Object::toString)
                     .collect(freezer());
    }

}
