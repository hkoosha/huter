package io.koosha.huter.main;

import io.koosha.huter.internal.HuterFiles;
import io.koosha.huter.runner.DefaultResultValidator;
import io.koosha.huter.runner.DefaultRunner;
import io.koosha.huter.runner.HuterRunner;
import io.koosha.huter.runner.HuterContext;
import io.koosha.huter.internal.HuterThrowables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public final class HuterMain {

    private static final Logger LOG = LoggerFactory.getLogger(HuterMain.class);

    private HuterMain() {
        throw new UnsupportedOperationException("utility class can not be instantiated.");
    }


    public static void main(final String... args) {

        Thread.currentThread().setName(HuterMain.class.getSimpleName());

        final List<String> errors;
        try {
            errors = run(args).getErrors();
        }
        catch (final Options.OptionsException e) {
            LOG.error(e.getMessage());
            System.exit(1);
            throw new IllegalStateException();
        }

        if (!errors.isEmpty()) {
            LOG.error("errors: {}", errors);
            System.exit(1);
        }
        else {
            LOG.info("all ok");
            System.exit(0);
        }
    }

    public static Result run(final String... args) throws Options.OptionsException {

        final HuterContext ctx;
        try {
            ctx = createContext(args);
        }
        catch (final Options.OptionsException e) {
            throw e;
        }
        catch (final Throwable e) {
            LOG.error("error", e);
            return Result.create(null, "huter_error: " + HuterThrowables.getMessage(e));
        }

        final List<Object[]> result;
        try (final HuterRunner hr = new DefaultRunner(ctx)) {
            result = hr.run();
        }
        catch (final Throwable e) {
            LOG.error("error", e);
            return Result.create(ctx, "huter_error: " + HuterThrowables.getMessage(e));
        }

        final List<String> errors = DefaultResultValidator.getInstance().apply(ctx.getName(), result);
        return Result.create(ctx, result, errors);
    }

    private static HuterContext createContext(final String... argz) throws IOException, Options.OptionsException {
        final Options ops = Options.parseArgs(argz);

        final HuterContext ctx = new HuterContext(
            ops.getRootPath(),
            ops.getName(),
            ops.getName());

        if (ops.getLogDir().isPresent())
            ctx.setLogDir(ops.getLogDir().get());

        if (ops.getTablesRootPath().isPresent()) {
            final Path path = ops.getTablesRootPath().get();
            if (!path.toFile().exists() || !path.toFile().isDirectory())
                throw new Options.OptionsException("tables definition root does not exist or is not a directory: " + path);
            ctx.setTableDefinitionsRootDir(path);
        }
        else {
            final boolean anyNonAbsolute = ops
                .getComponentFilePath()
                .stream()
                .flatMap(it -> {
                    try {
                        return HuterFiles.readAllLines(it).stream();
                    }
                    catch (final IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .filter(it -> it.startsWith("file"))
                .map(it -> it.split("\\s", 2))
                .peek(it -> {
                    if (it.length != 2)
                        throw new RuntimeException("invalid table definition line: " + Arrays.toString(it));
                })
                .map(it -> it[1])
                .map(Paths::get)
                .anyMatch(p -> !p.isAbsolute());
            if (anyNonAbsolute)
                throw new Options.OptionsException("a relative table definition file given but table definition root is not set");
        }

        if (ops.getQueryFilePath().isPresent())
            ctx.setQueryFile(ops.getQueryFilePath().get());
        else if (ops.getQuery().isPresent())
            ctx.setQuery(ops.getQuery().get());
        else
            LOG.info("no query set, skipping query");

        if (ops.getTestQueryFilePath().isPresent())
            ctx.setTestQueryFile(ops.getTestQueryFilePath().get());
        else if (ops.getTestQuery().isPresent())
            ctx.setTestQuery(ops.getTestQuery().get());
        else
            LOG.info("no test query set, skipping test query");

        for (final Path tableFilePath : ops.getComponentFilePath())
            ctx.addTablesFile(tableFilePath);
        for (final String table : ops.getComponentsQueries())
            ctx.addTable(table);

        for (final Path paramFilePath : ops.getParamFilePaths())
            ctx.addParameterFile(paramFilePath);
        for (final String param : ops.getParamQueries())
            ctx.addParameterFilesContent(param);

        for (final Path setupFilePath : ops.getSetupFilePaths())
            ctx.addSetupFile(setupFilePath);
        for (final String setup : ops.getSetupQueries())
            ctx.addSetupFileContent(setup);

        return ctx;
    }

}
