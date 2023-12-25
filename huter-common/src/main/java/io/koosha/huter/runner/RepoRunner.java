package io.koosha.huter.runner;

import io.koosha.huter.internal.CloseableManager;
import io.koosha.huter.internal.HuterFiles;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class RepoRunner extends CloseableManager implements HuterRunner {

    private static final Logger LOG = LoggerFactory.getLogger(RepoRunner.class);

    public static final String REPO_RUNNER_PARAMETERS_INI = "parameters.ini";
    public static final String REPO_RUNNER_SETUP_SQL_FILE = "setup.hql";
    public static final String REPO_RUNNER_TABLE_LIST_FILE = "dependencies.txt";
    public static final String REPO_RUNNER_TEST_DIR_NAME = "test";
    public static final String REPO_RUNNER_OUT_DIR = "out";
    public static final String TEST_CASE_PREFIX = "test_";
    public static final String HIVE_SUFFIX = ".hql";

    private final Path rootDir;
    private final Path testSubDir;
    private final Path outSubDir;

    public RepoRunner(final String rootDir) {

        // HACK!
        // Currently, if we don't do this, ObjectStore closes pmf while there's an active TX, which fails.
        ObjectStore.setTwoMetastoreTesting(true);

        this.rootDir = Paths.get(rootDir);
        this.testSubDir = this.rootDir.resolve(REPO_RUNNER_TEST_DIR_NAME);
        this.outSubDir = this.testSubDir.resolve(REPO_RUNNER_OUT_DIR);
    }

    @Override
    public List<Object[]> run() throws Exception {

        final List<String> errors = this.executeTestSuits();
        return Collections.singletonList(errors.toArray());
    }


    private boolean containsAnyTest(final Path path) {

        try (final Stream<Path> potentialWalk = Files.walk(path)) {

            return potentialWalk.anyMatch(potentialTestScript -> {
                if (!potentialTestScript.getFileName().toString().startsWith(TEST_CASE_PREFIX)) {
                    LOG.trace("rejected, not starting with hive prefix: {}", potentialTestScript);
                    return false;
                }
                else if (!potentialTestScript.getFileName().toString().endsWith(HIVE_SUFFIX)) {
                    LOG.trace("rejected, not ending with hive suffix: {}", potentialTestScript);
                    return false;
                }
                else if (!Files.isRegularFile(potentialTestScript)) {
                    LOG.trace("rejecting as it is not a regular file: {}", potentialTestScript);
                    return false;
                }
                else {
                    return true;
                }
            });

        }
        catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean isTestSuite(final Path path) {
        if (!path.getFileName().toString().endsWith(HIVE_SUFFIX)) {
            LOG.trace("rejected, not ending with hive suffix: {}", path);
            return false;
        }
        else if (!Files.isDirectory(path)) {
            LOG.trace("rejecting as test location is not a directory: {}", path);
            return false;
        }

        return true;
    }

    private List<String> executeTestSuits() throws Exception {

        final Collection<Path> testSuits;

        try (final Stream<Path> testSubDirWalk = Files.walk(this.testSubDir)) {
            testSuits = testSubDirWalk
                    .filter(it -> this.isTestSuite(it) && this.containsAnyTest(it))
                    .peek(path -> LOG.trace("found potential test suit: {}", path))
                    .collect(Collectors.toList());
        }

        LOG.trace("executing test suits: {}", testSuits);

        final List<String> errors = new ArrayList<>();
        for (final Path testSuit : testSuits)
            this.executeTestSuit(errors, testSuit);

        LOG.info("testSuites={} paths={}", testSuits.size(), testSuits);

        return errors;
    }

    private void executeTestSuit(final Collection<String> errors,
                                 final Path testSuitBaseDir) throws Exception {

        LOG.info("executing test suit={}", testSuitBaseDir);
        HuterFiles.assertIsAbsolute(testSuitBaseDir);

        final List<Path> testModules = HuterFiles.subDirectoriesOf(testSuitBaseDir);
        for (final Path testModule : testModules)
            this.executeTestModule(errors, testSuitBaseDir, testModule);
    }

    private void executeTestModule(final Collection<String> errors,
                                   final Path testSuit,
                                   final Path testModule) throws Exception {

        LOG.info("executing test module={}", testModule);

        final List<Path> testCases = HuterFiles.subFilesOf(testModule, path ->
                path.getFileName().toString().toLowerCase().startsWith(TEST_CASE_PREFIX)
                        && path.getFileName().toString().toLowerCase().endsWith(HIVE_SUFFIX)
        );

        for (final Path testCase : testCases)
            this.executeTestCase(errors, testSuit, testModule, testCase);
    }

    private void executeTestCase(final Collection<String> errors,
                                 final Path testSuit,
                                 final Path testModule,
                                 final Path testCase) throws Exception {

        final HuterContext ctx = this.createCtx(testSuit, testModule, testCase);

        final List<Object[]> result;
        try (final HuterRunner runner = DefaultRunner.of(ctx)) {
            result = runner.run();
        }

        final List<String> e = DefaultResultValidator.getInstance().apply(ctx.getName(), result);
        errors.addAll(e);
    }


    private HuterContext createCtx(final Path testSuit,
                                   final Path testModule,
                                   final Path validatorScript) throws Exception {

        final HuterContext ctx = this.initCtx(testSuit, testModule, validatorScript);

        this.initParameters(ctx, testSuit, testModule);
        this.initTable(ctx, testSuit);
        this.initSetup(ctx, testSuit, testModule);
        this.initSetup(ctx, testSuit, testModule);

        return ctx;
    }

    private HuterContext initCtx(
            Path testSuit,
            Path testModule,
            Path validatorScript) throws IOException {

        final Path dataDir = this.outSubDir.resolve(this.testSubDir.relativize(testModule));

        final HuterContext ctx = new HuterContext(
                this.rootDir,
                validatorScript.toString(),
                validatorScript.getFileName().toString().replace(".hql", "")
        );

        ctx.setOutDir(this.outSubDir);
        ctx.setTableDefinitionsRootDir(this.rootDir);
        ctx.setHiveBaseDir(this.outSubDir);
        ctx.setLogDir(dataDir.resolve("logs"));
        ctx.setDataDir(dataDir.resolve("table_data"));
        ctx.setQueryFile(this.rootDir.resolve(this.testSubDir.relativize(testSuit)));
        ctx.setTestQueryFile(validatorScript);

        return ctx;
    }

    private void initParameters(HuterContext ctx,
                                Path testSuit,
                                Path testModule) {

        // parameters.ini file.
        try {
            ctx.addParameterFile(testSuit.resolve(REPO_RUNNER_PARAMETERS_INI));
        }
        catch (final IOException e) {
            LOG.warn("could not load parameters file, ignoring: {}", e.getMessage());
        }
        try {
            ctx.addParameterFile(testModule.resolve(REPO_RUNNER_PARAMETERS_INI));
        }
        catch (final IOException e) {
            LOG.warn("could not load parameters file, ignoring: {}", e.getMessage());
        }
    }

    private void initTable(HuterContext ctx, Path testSuit) {

        // Tables file.
        try {
            ctx.addTablesFile(testSuit.resolve(REPO_RUNNER_TABLE_LIST_FILE));
        }
        catch (final IOException e) {
            LOG.warn("could not load tables file, ignoring: {}", e.getMessage());
        }
    }

    private void initSetup(HuterContext ctx,
                           Path testSuit,
                           Path testModule) {

        // Setup file.
        try {
            ctx.addSetupFile(testSuit.resolve(REPO_RUNNER_SETUP_SQL_FILE));
        }
        catch (final IOException e) {
            LOG.warn("could not load setup file, ignoring: {}", e.getMessage());
        }
        try {
            ctx.addSetupFile(testModule.resolve(REPO_RUNNER_SETUP_SQL_FILE));
        }
        catch (final IOException e) {
            LOG.warn("could not load setup file, ignoring: {}", e.getMessage());
        }
    }

}
