package com.trivago.huter.runner;

import com.trivago.huter.util.CloseableManager;
import com.trivago.huter.util.HuterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.trivago.huter.util.HuterUtil.freeze;

public final class RepoRunner extends CloseableManager implements HuterRunner {

    private static final Logger LOG = LoggerFactory.getLogger(RepoRunner.class);

    public static final String REPO_RUNNER_PARAMETERS_INI = "parameters.ini";
    public static final String REPO_RUNNER_SETUP_SQL_FILE = "setup.hql";
    public static final String REPO_RUNNER_TABLE_LIST_FILE = "table_list.txt";
    public static final String REPO_RUNNER_TEST_DIR_NAME = "test";
    public static final String REPO_RUNNER_OUT_DIR = "out";

    private final Path hadoopWfDir;
    private final Path testSubDir;
    private final Path outSubDir;

    public RepoRunner(final String hadoopWfDir) {
        this.hadoopWfDir = Paths.get(hadoopWfDir);
        this.testSubDir = this.hadoopWfDir.resolve(REPO_RUNNER_TEST_DIR_NAME);
        this.outSubDir = this.testSubDir.resolve(REPO_RUNNER_OUT_DIR);
    }

    @Override
    public List<Object[]> run() throws Exception {
        final List<String> errors = new ArrayList<>();
        this.executeTestSuits(errors);

        final List<Object[]> errors0 = new ArrayList<>(1);
        errors0.add(errors.toArray());
        return freeze(errors0);
    }


    private void executeTestSuits(Collection<String> errors) throws Exception {
        final Collection<Path> testSuits;
        try (final Stream<Path> walk = Files.walk(this.testSubDir)) {
            testSuits = walk
                .filter(it -> {
                    final Path potentialQueryFile = this.hadoopWfDir.resolve(this.testSubDir.relativize(it));
                    final boolean potentialQueryFileIsFile = Files.isRegularFile(potentialQueryFile);
                    final boolean testLocationIsDir = Files.isDirectory(it);
                    return testLocationIsDir && potentialQueryFileIsFile;
                })
                .peek(path -> LOG.trace("found potential test suit: {}", path))
                .collect(Collectors.toList());
        }

        LOG.trace("executing test suits: {}", testSuits);
        for (final Path testSuit : testSuits)
            this.executeTestSuit(errors, testSuit);

        LOG.info("{} {}", testSuits.size(), testSuits);
    }

    private void executeTestSuit(final Collection<String> errors,
                                 final Path testSuitBaseDir) throws Exception {
        LOG.info("executing test suit={}", testSuitBaseDir);
        HuterUtil.assertIsAbsolute(testSuitBaseDir);

        final List<Path> testModules = HuterUtil.subDirectoriesOf(testSuitBaseDir);
        for (final Path testModule : testModules)
            this.executeTestModule(errors, testSuitBaseDir, testModule);
    }

    private void executeTestModule(final Collection<String> errors,
                                   final Path testSuit,
                                   final Path testModule) throws Exception {
        LOG.info("executing test module={}", testModule);

        final List<Path> testCases = HuterUtil.subFilesOf(testModule, path ->
            path.getFileName().toString().toLowerCase().startsWith("test")
                && path.getFileName().toString().toLowerCase().endsWith(".hql"));

        for (final Path testCase : testCases)
            this.executeTestCase(errors, testSuit, testModule, testCase);
    }

    private void executeTestCase(final Collection<String> errors,
                                 final Path testSuit,
                                 final Path testModule,
                                 final Path testCase) throws Exception {
        final HuterContext ctx = this.createCtx(testSuit, testModule, testCase);

        final List<Object[]> result;
        try (final DefaultRunner runner = new DefaultRunner(ctx)) {
            result = runner.run();
        }

        final List<String> e = DefaultResultValidator.getInstance().apply(ctx.getName(), result);
        errors.addAll(e);
    }


    private HuterContext createCtx(final Path testSuit,
                                   final Path testModule,
                                   final Path validatorScript) throws Exception {
        final Path dataDir = this.outSubDir.resolve(this.testSubDir.relativize(testModule));

        final HuterContext ctx = new HuterContext(
            this.hadoopWfDir,
            validatorScript.toString(),
            validatorScript.getFileName().toString().replace(".hql", "")
        );
        ctx.setOutDir(this.outSubDir);
        ctx.setTableDefinitionsRootDir(this.hadoopWfDir);
        ctx.setHiveBaseDir(this.outSubDir);
        ctx.setLogDir(dataDir.resolve("logs"));
        ctx.setDataDir(dataDir.resolve("table_data"));
        ctx.setQueryFile(this.hadoopWfDir.resolve(this.testSubDir.relativize(testSuit)));
        ctx.setTestQueryFile(validatorScript);

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

        // Tables file.
        try {
            ctx.addTablesFile(testSuit.resolve(REPO_RUNNER_TABLE_LIST_FILE));
        }
        catch (final IOException e) {
            LOG.warn("could not load tables file, ignoring: {}", e.getMessage());
        }

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

        return ctx;
    }

}