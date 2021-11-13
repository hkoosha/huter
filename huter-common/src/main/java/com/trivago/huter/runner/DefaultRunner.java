package com.trivago.huter.runner;

import com.trivago.huter.TableLocationFixerHook;
import com.trivago.huter.component.ComponentCreatorHub;
import com.trivago.huter.util.CloseableManager;
import com.trivago.huter.util.HuterUtil;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.HiveSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public final class DefaultRunner extends CloseableManager implements HuterRunner {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRunner.class);

    public static final String HUTER_OUTPUT_FILE = "huter_out";

    private final HuterContext ctx;
    private final ComponentCreatorHub componentCreatorHub;

    public DefaultRunner(final HuterContext ctx) {
        this.ctx = Objects.requireNonNull(ctx);

        this.componentCreatorHub = new ComponentCreatorHub(path ->
            HuterUtil.readFile(path.isAbsolute()
                ? path
                : ctx.getTableDefinitionsRootDir().resolve(path)));
    }

    @Override
    public List<Object[]> run() throws Exception {
        LOG.info("init");
        this.init();

        LOG.info("creating components");
        this.createComponents();

        LOG.info("setup");
        this.setup();

        LOG.info("execute");
        this.execute();

        LOG.info("generating results");
        final List<Object[]> result = this.test();

        LOG.info("writing results");
        this.write();

        return result;
    }

    // ------------------------------------------------------------------- INIT

    private void init() throws Exception {
        LOG.trace("opening context");

        this.initDirs();
        this.initMetastore();
        this.initConfigureHive();
        this.initDeadline();
        this.initSession();
        this.initCleanAndFixUpMetaStore();
        this.initUpdateParametersInHiveSession(this.ctx.getParametersProperties());
        this.initSetCliDriver();
    }

    private void initMetastore() throws Exception {
        final String[] sql = String.join("\n", Files.readAllLines(
            Paths.get("/opt/hive3/scripts/metastore/upgrade/derby/hive-schema-3.1.0.derby.sql")))
                                   .replace("\"APP\".", "")
                                   .split(";");
        try (final Connection conn = DriverManager.getConnection(this.ctx.getConnectionStr())) {
            for (final String s : sql) {
                try (final Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(s);
                }
            }
        }
    }

    private void initDirs() throws IOException {
        HuterUtil.ensureDirectories(this.ctx.getOutDir());
        HuterUtil.ensureDirectories(this.ctx.getHiveJarDir());
    }

    private void initConfigureHive() {
        LOG.info("configuring hive");
        this.ctx.setHiveConf(new HiveConf());
        final HiveConf hc = this.ctx.getHiveConf();

        DefaultRunnerConfigurator.configureHive(this.ctx, hc);
        DefaultRunnerConfigurator.configureHiveExperimentalOptions(hc);
        DefaultRunnerConfigurator.configureTez(hc);
        DefaultRunnerConfigurator.configureFs(hc, this.ctx);
        DefaultRunnerConfigurator.configureDerby(hc);

        hc.setVar(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK, TableLocationFixerHook.class.getName());

        if (LOG.isTraceEnabled())
            LOG.trace("final hive configuration: {}", hc.getAllProperties());
    }

    private void initDeadline() throws MetaException {
        Deadline.registerIfNot(Integer.MAX_VALUE);
        Deadline.startTimer("something");
    }

    private void initSession() throws HiveSQLException {
        this.ctx.init(this.ctx.getHiveConf());
        this.addClosable(this.ctx);

        LOG.info("starting new session state.");
        // final CliSessionState cliSession = new CliSessionState(this.ctx.getHiveConf());
        // SessionState.start(cliSession);
        // SessionState.setCurrentSessionState(cliSession);

        // TODO needed?
        // this.addClosable(() -> SessionState.get().close());
    }

    @SuppressWarnings("SpellCheckingInspection")
    private void initCleanAndFixUpMetaStore() throws SQLException {
        try (final Connection conn = DriverManager.getConnection(this.ctx.getConnectionStr());
             final Statement stmt = conn.createStatement()) {
            stmt.execute(
                "ALTER TABLE APP.COLUMNS_V2 " +
                    "ALTER COLUMN COMMENT " +
                    "SET DATA TYPE varchar(8096)");
        }

        // final List<String> tablesToTruncate = new ArrayList<>();
        // try (final Connection conn = DriverManager.getConnection(this.ctx.getConnectionStr());
        //      final Statement stmt = conn.createStatement()) {
        //     //noinspection SpellCheckingInspection
        //     stmt.execute(
        //         "select TABLENAME,SCHEMANAME " +
        //             "from SYS.SYSTABLES TBL " +
        //             "join SYS.SYSSCHEMAS SMA " +
        //             "on (TBL.SCHEMAID = SMA.SCHEMAID)");
        //
        //     try (final ResultSet rs = stmt.getResultSet()) {
        //         while (rs.next())
        //             if ("APP".equals(rs.getString("SCHEMANAME")))
        //                 tablesToTruncate.add(rs.getString("TABLENAME"));
        //     }
        // }
        //
        // try (final Connection conn = DriverManager.getConnection(this.ctx.getConnectionStr())) {
        //     for (final String table : tablesToTruncate)
        //         try (final Statement stmt = conn.createStatement()) {
        //             stmt.execute("TRUNCATE TABLE " + table);
        //         }
        // }
    }

    private void initUpdateParametersInHiveSession(final Properties properties) {
        final HashMap<String, String> asStringMap = new HashMap<>();
        properties.forEach((key, value) -> asStringMap.put(key.toString(), value.toString()));
        this.ctx.getCurrentSessionState().setHiveVariables(asStringMap);
    }

    private void initSetCliDriver() {
        this.ctx.setDriver(new CliDriver());
    }

    // ---------------------------------------------------------------- EXECUTE

    private void createComponents() throws Exception {
        for (final String table : this.ctx.getTables())
            this.componentCreatorHub.createComponent(this.ctx, this.ctx.getDataDir(), table);
    }

    private void setup() throws HiveSQLException {
        for (final String setup : this.ctx.getSetupFilesContent()) {
            final List<Object[]> result = this.ctx.executeSql(setup);
            LOG.debug("setup result: {}", result);
        }
    }

    private void execute() throws HiveSQLException {
        if (!ctx.getQuery().isPresent())
            return;

        SessionState.setCurrentSessionState(this.ctx.getCurrentSessionState());
        final List<Object[]> result = this.ctx.executeSql(this.ctx.getQuery().get());
        LOG.debug("execute result: {}", result);
    }

    private List<Object[]> test() throws HiveSQLException {
        if (!ctx.getTestQuery().isPresent())
            return Collections.emptyList();

        final List<Object[]> result = this.ctx.executeSql(this.ctx.getTestQuery().get());
        LOG.debug("test query result: {}", result);
        this.ctx.setTestResult(result.stream()
                                     .map(Object[]::clone)
                                     .collect(Collectors.toList()));
        return result;
    }

    private void write() throws IOException {
        if (!this.ctx.getLogDir().isPresent()) {
            LOG.info("not persisting any output as logDir is not set");
            return;
        }

        final Path logDir = this.ctx.getLogDir().get();
        LOG.trace("writing test output to file={}", logDir);

        this.ctx.getHuterOutput()
                .writeUtf8("\n================> TEST [")
                .writeUtf8(ctx.getName())
                .writeUtf8("] ==================>\n")
                .writeUtf8(this.ctx.getTestQuery().isPresent() ? this.ctx.getTestQuery().get() : "NONE")
                .writeUtf8("\n\n")
                .writeUtf8("\n================> RESULT [")
                .writeUtf8(ctx.getName())
                .writeUtf8("] ================>\n");
        for (final Object[] objects : ctx.getTestResult())
            this.ctx.getHuterOutput()
                    .writeUtf8(Arrays.toString(objects))
                    .writeUtf8("\n");
        this.ctx.getHuterOutput()
                .writeUtf8("\n\n")
                .writeUtf8("================> END [")
                .writeUtf8(ctx.getName())
                .writeUtf8("] ===================>\n");

        final String target = HUTER_OUTPUT_FILE
            + "__"
            + ctx.getShortName()
            + ".txt";
        HuterUtil.appendToFile(ctx.getHuterOutput(), logDir, target);
    }

}
