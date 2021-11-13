package com.trivago.huter.main;

import picocli.CommandLine;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import static com.trivago.huter.util.HuterUtil.filter;
import static com.trivago.huter.util.HuterUtil.freezer;

@SuppressWarnings("unused")
@CommandLine.Command(name = "HiveUnitTestRunner",
        mixinStandardHelpOptions = true,
        version = "Huter 0.1")
class Options implements Callable<Integer> {

    private static final String DEFAULT_NAME = "unnamed";

    private static final ThreadLocal<Options> parsed = new ThreadLocal<>();

    private Options() {
    }


    @CommandLine.Option(names = {"--name", "-n"},
            defaultValue = DEFAULT_NAME)
    private String name;


    @CommandLine.Option(names = {"--root", "-r"},
            required = true)
    private String root;


    @CommandLine.Option(names = {"--table-definitions-root", "-d"},
            defaultValue = "")
    private String tablesRoot;


    @CommandLine.Option(names = {"--log-dir", "-g"},
            defaultValue = "")
    private String logDir;

    @CommandLine.Option(names = {"--query-file", "-Q"},
            defaultValue = "")
    private String queryFile;

    @CommandLine.Option(names = {"--query", "-q"},
            defaultValue = "")
    private String query;

    @CommandLine.Option(names = {"--test-query-file", "-T"},
            defaultValue = "")
    private String testQueryFile;

    @CommandLine.Option(names = {"--test-query", "-t"},
            defaultValue = "")
    private String testQuery;


    @CommandLine.Option(names = {"--setup-file", "-S"},
            defaultValue = "")
    private List<String> setupFiles;

    @CommandLine.Option(names = {"--setup-query", "-s"},
            defaultValue = "")
    private List<String> setupQueries;


    @CommandLine.Option(names = {"--component-file", "-L"},
            defaultValue = "")
    private List<String> componentsFiles;

    @CommandLine.Option(names = {"--component-query", "-l"},
            defaultValue = "")
    private List<String> componentsQueries;


    @CommandLine.Option(names = {"--param-file", "-P"},
            defaultValue = "")
    private List<String> paramFiles;

    @CommandLine.Option(names = {"--param-query", "-p"},
            defaultValue = "")
    private List<String> paramQueries;


    String getName() {
        return this.name;
    }


    Path getRootPath() {
        return Paths.get(this.root.trim());
    }


    Optional<Path> getTablesRootPath() {
        return this.tablesRoot.trim().isEmpty()
                ? Optional.empty()
                : Optional.of(Paths.get(this.tablesRoot.trim()));
    }


    Optional<Path> getQueryFilePath() {
        return this.queryFile.trim().isEmpty()
                ? Optional.empty()
                : Optional.of(Paths.get(this.queryFile.trim()));
    }

    Optional<String> getQuery() {
        return this.query.trim().isEmpty()
                ? Optional.empty()
                : Optional.of(this.query.trim());
    }

    Optional<Path> getTestQueryFilePath() {
        return this.testQueryFile.trim().isEmpty()
                ? Optional.empty()
                : Optional.of(Paths.get(this.testQueryFile.trim()));
    }

    Optional<String> getTestQuery() {
        return this.testQuery.trim().isEmpty()
                ? Optional.empty()
                : Optional.of(this.testQuery.trim());
    }

    Optional<Path> getLogDir() {
        return this.logDir.trim().isEmpty()
                ? Optional.empty()
                : Optional.of(Paths.get(this.logDir.trim()));
    }


    List<Path> getSetupFilePaths() {
        return toPath(filter(this.setupFiles));
    }

    List<Path> getComponentFilePath() {
        return toPath(filter(this.componentsFiles));
    }

    List<Path> getParamFilePaths() {
        return toPath(filter(this.paramFiles));
    }

    List<String> getSetupQueries() {
        return filter(this.setupQueries);
    }

    List<String> getComponentsQueries() {
        return filter(this.componentsQueries);
    }

    List<String> getParamQueries() {
        return filter(this.paramQueries);
    }


    @Override
    public Integer call() {
        Options.parsed.set(this);
        return 0;
    }

    private Optional<String> getError() {
        if (this.getRootPath().toFile().exists())
            return Optional.of("error: root directory already exists: " + this.getRootPath());

        if (this.getTablesRootPath().isPresent() && !this.getTablesRootPath().get().toFile().exists())
            return Optional.of("error: table definitions directory does not exists: " + this.getTablesRootPath());


        if (this.getQueryFilePath().isPresent() && this.getQuery().isPresent())
            return Optional.of("error: can not set both query and query file.");
        if (this.getQueryFilePath().isPresent() && !this.getQueryFilePath().get().toFile().exists())
            return Optional.of("error: query file does not exits: " + this.getQueryFilePath().get());

        if (this.getTestQueryFilePath().isPresent() && this.getTestQuery().isPresent())
            return Optional.of("error: can not set both testQuery and testQuery file.");
        if (this.getTestQueryFilePath().isPresent() && !this.getTestQueryFilePath().get().toFile().exists())
            return Optional.of("error: testQuery file does not exits: " + this.getQueryFilePath().get());


        if (!nonExisting(this.getSetupFilePaths()).isEmpty())
            return Optional.of("error: setup files does not exist: " + nonExisting(this.getSetupFilePaths()));

        if (!nonExisting(this.getComponentFilePath()).isEmpty())
            return Optional.of("error: component files does not exist: " + nonExisting(this.getComponentFilePath()));

        if (!nonExisting(this.getParamFilePaths()).isEmpty())
            return Optional.of("error: param files does not exist: " + nonExisting(this.getParamFilePaths()));

        return Optional.empty();
    }


    private static List<Path> toPath(final Collection<String> source) {
        return source.stream()
                     .map(Paths::get)
                     .collect(freezer());
    }

    private static List<Path> nonExisting(final Collection<Path> source) {
        return source.stream()
                     .filter(it -> !it.toFile().exists())
                     .collect(freezer());
    }


    static Options parseArgs(final String... args) throws OptionsException {
        final int execute = new CommandLine(new Options()).execute(args);
        final Options instance = Options.parsed.get();
        Options.parsed.set(null);

        if (execute != 0)
            throw new OptionsException("error parsing command line");

        if (instance == null)
            throw new OptionsException("no options");

        if (instance.getError().isPresent())
            throw new OptionsException(instance.getError().get());

        return instance;
    }


    static final class OptionsException extends Exception {
        public OptionsException(final String message) {
            super(message);
        }
    }

}
