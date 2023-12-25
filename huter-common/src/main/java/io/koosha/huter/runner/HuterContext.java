package io.koosha.huter.runner;

import io.koosha.huter.util.HuterUtil;
import io.koosha.huter.util.StringOutputStream;
import org.apache.hadoop.hive.cli.CliDriver;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.tez.TezJobExecHelper;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.koosha.huter.util.HuterUtil.freeze;
import static io.koosha.huter.util.HuterUtil.freezer;
import static java.util.Collections.singleton;

public final class HuterContext implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HuterContext.class);

    public static final String HIVE_JAR_DIR = "jars";
    public static final String HIVE_SCRATCH_DIR = "scratch";
    public static final String HIVE_WAREHOUSE = "warehouse";
    public static final String DERBY_CONN_STRING_IN_MEM = "jdbc:derby:memory:metastore_db;create=true";

    private final String name;
    private final String shortName;
    private final String dbName;
    private final Path rootDirectory;
    private Path tableDefinitionsRootDir;

    private Path logDir;
    private Path outDir;
    private Path dataDir;

    private Path hiveBaseDir;
    private Path hiveWarehouseDir;
    private Path hiveJarDir;
    private Path hiveScratchDir;

    private final List<String> setupFiles = new ArrayList<>();
    private final List<String> parameterFiles = new ArrayList<>();
    private final Set<String> tables = new LinkedHashSet<>();

    private HiveConf hiveConf;
    private String query;
    private String testQuery;

    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private CliDriver driver;
    private CLIService client;
    private HiveServer2 hiveServer2;
    private SessionHandle sessionHandle;
    private SessionState currentSessionState;

    private final StringOutputStream huterOutput = StringOutputStream.forUtf8();
    private List<Object[]> testResult;

    public HuterContext(final Path workDir,
                        final String name,
                        final String shortName) {
        this.rootDirectory = Objects.requireNonNull(workDir);
        this.name = name;
        this.shortName = shortName;
        this.dbName = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

    @Override
    public String toString() {
        final String name = this.getName() == null ? "?" : this.getName();
        return "HuterContext[name=" + name + "]";
    }

    public String getName() {
        return this.name;
    }

    public String getShortName() {
        return this.shortName;
    }

    public Path getRootDirectory() {
        return this.rootDirectory;
    }

    public void setHiveConf(final HiveConf hiveConf) {
        this.hiveConf = hiveConf;
    }

    public HiveConf getHiveConf() {
        return this.hiveConf;
    }

    public void setDriver(final CliDriver driver) {
        this.driver = driver;
    }

    public SessionState getCurrentSessionState() {
        return this.currentSessionState;
    }


    public void addSetupFile(final Path path) throws IOException {
        final String content = HuterUtil.readFile(path);
        this.addSetupFileContent(content);
    }

    public void addSetupFileContent(final String content) {
        Objects.requireNonNull(content);
        this.setupFiles.add(content);
    }

    public List<String> getSetupFilesContent() {
        return HuterUtil.filter(this.setupFiles);
    }


    public void addParameterFile(final Path path) throws IOException {
        final String content = HuterUtil.readFile(path);
        this.addParameterFilesContent(content);
    }

    public void addParameterFilesContent(final String content) {
        Objects.requireNonNull(content);
        this.parameterFiles.add(content);
    }

    public List<String> getParameterFilesContent() {
        return freeze(this.parameterFiles);
    }

    public Properties getParametersProperties() {
        final Properties properties = new Properties();
        for (final String prop : HuterUtil.filter(getParameterFilesContent()))
            try {
                properties.load(new StringReader(prop));
            }
            catch (final IOException e) {
                // Can't happen
                throw new RuntimeException(e);
            }
        return properties;
    }


    public void addTablesFile(final Path path) throws IOException {
        this.addTables(HuterUtil.readAllLines(path));
    }

    public void addTable(final String table) {
        this.addTables(singleton(table));
    }

    public void addTables(final Collection<String> tables) {
        this.tables.addAll(tables);
    }

    public Collection<String> getTables() {
        return freeze(new LinkedHashSet<>(HuterUtil.filter(this.tables)));
    }


    public void setQueryFile(final Path path) throws IOException {
        final String content = HuterUtil.readFile(path);
        this.setQuery(content);
    }

    public void setQuery(final String content) {
        if (this.query != null && content != null)
            LOG.warn("query file not empty! overriding. Is this intended?");
        this.query = content;
    }

    public Optional<String> getQuery() {
        return this.query == null || this.query.trim().isEmpty()
                ? Optional.empty()
                : Optional.of(this.query);
    }


    public void setTestQueryFile(final Path path) throws IOException {
        final String content = HuterUtil.readFile(path);
        this.setTestQuery(content);
    }

    public void setTestQuery(final String content) {
        if (this.testQuery != null && content != null && !content.isEmpty())
            LOG.warn("testQuery file not empty! overriding. Is this intended?");
        this.testQuery = content;
    }

    public Optional<String> getTestQuery() {
        return this.testQuery == null || this.testQuery.trim().isEmpty()
                ? Optional.empty()
                : Optional.of(this.testQuery);
    }


    public void setTableDefinitionsRootDir(final Path tableDefinitionsRootDir) {
        this.tableDefinitionsRootDir = tableDefinitionsRootDir;
    }

    public Path getTableDefinitionsRootDir() {
        if (this.tableDefinitionsRootDir == null)
            throw new IllegalStateException("table definition root directory not set");
        return this.tableDefinitionsRootDir;
    }


    public Path getOutDir() {
        return this.outDir == null ? this.getRootDirectory() : this.outDir;
    }

    public void setOutDir(final Path outDir) {
        this.outDir = outDir;
    }


    public Optional<Path> getLogDir() {
        return Optional.ofNullable(this.logDir);
    }

    public void setLogDir(final Path logDir) {
        this.logDir = logDir;
    }


    public Path getDataDir() {
        return this.dataDir == null ? this.getRootDirectory() : this.dataDir;
    }

    public void setDataDir(final Path dataDir) {
        this.dataDir = dataDir;
    }


    public Path getHiveBaseDir() {
        return this.hiveBaseDir == null ? this.getRootDirectory() : this.hiveBaseDir;
    }

    public void setHiveBaseDir(final Path hiveBaseDir) {
        this.hiveBaseDir = hiveBaseDir;
    }


    public Path getHiveJarDir() {
        return this.hiveJarDir == null ? this.getHiveBaseDir().resolve(HIVE_JAR_DIR) : this.hiveJarDir;
    }

    @SuppressWarnings("unused")
    public void setHiveJarDir(final Path hiveJarDir) {
        this.hiveJarDir = hiveJarDir;
    }


    public Path getHiveScratchDir() {
        return this.hiveScratchDir == null ? this.getHiveBaseDir().resolve(HIVE_SCRATCH_DIR) : this.hiveScratchDir;
    }

    @SuppressWarnings("unused")
    public void setHiveScratchDir(final Path hiveScratchDir) {
        this.hiveScratchDir = hiveScratchDir;
    }


    public Path getHiveWareHouseDir() {
        return this.hiveWarehouseDir == null ? this.getHiveBaseDir().resolve(HIVE_WAREHOUSE) : this.hiveWarehouseDir;
    }

    @SuppressWarnings("unused")
    public void setHiveWarehouseDir(final Path hiveWarehouseDir) {
        this.hiveWarehouseDir = hiveWarehouseDir;
    }


    public String getDbName() {
        return this.dbName;
    }

    public String getConnectionStr() {
        return DERBY_CONN_STRING_IN_MEM
                .replace("metastore_db", "metastore_db_" + this.getDbName());
    }


    public StringOutputStream getHuterOutput() {
        return this.huterOutput;
    }

    public List<Object[]> getTestResult() {
        return this.testResult.stream()
                              .map(Object[]::clone)
                              .collect(Collectors.toList());
    }

    public void setTestResult(final List<Object[]> testResult) {
        this.testResult = testResult.stream()
                                    .map(Object[]::clone)
                                    .collect(Collectors.toList());
    }

    // ------------------------------------------------------------------------

    public void init(final HiveConf hc) throws HiveSQLException {
        Objects.requireNonNull(hc);

        this.hiveServer2 = new HiveServer2();
        hiveServer2.init(hc);

        this.client = hiveServer2.getServices()
                                 .stream()
                                 .filter(it -> it instanceof CLIService)
                                 .map(it -> (CLIService) it)
                                 .findFirst()
                                 .orElseThrow(() -> new IllegalStateException("could not find cli service"));

        this.sessionHandle = client.openSession("noUser", "noPassword", null);
        this.currentSessionState = client.getSessionManager()
                                         .getSession(sessionHandle)
                                         .getSessionState();
    }

    public List<Object[]> executeSql(final String sql) throws HiveSQLException {
        final List<Object[]> results = new ArrayList<>();
        for (final String statement : splitSemiColon(sql))
            results.addAll(this.executeSql0(statement));
        return freeze(results);
    }

    private List<Object[]> executeSql0(final String sql) throws HiveSQLException {
        final OperationHandle handle;
        try {
            handle = client.executeStatement(sessionHandle, sql, new HashMap<>());
        }
        catch (HiveSQLException e) {
            LOG.error("statement failed: {}", sql.trim(), e);
            throw e;
        }

        final List<Object[]> resultSet = new ArrayList<>();
        if (handle.hasResultSet()) {
            RowSet rowSet;
            while ((rowSet = client.fetchResults(handle)) != null && rowSet.numRows() > 0)
                for (final Object[] row : rowSet)
                    resultSet.add(row.clone());
        }

        return resultSet;
    }

    @Override
    public void close() throws Exception {
        LOG.info("closing tez");
        Throwable t = null;
        try {
            TezJobExecHelper.killRunningJobs();
        }
        catch (final Throwable err) {
            t = err;
        }

        // Will mess up logging if uncommented.
        // LOG.info("closing sessionHandle");
        // if (this.client != null)
        //     try {
        //         this.client.closeSession(this.sessionHandle);
        //     }
        //     catch (final Throwable err) {
        //         t = HuterUtil.merge(t, err);
        //     }

        LOG.info("closing currentSessionState");
        if (this.currentSessionState != null)
            try {
                this.currentSessionState.close();
            }
            catch (final Throwable err) {
                t = HuterUtil.merge(t, err);
            }

        LOG.info("closing hiveServer2");
        if (this.hiveServer2 != null)
            try {
                this.hiveServer2.stop();
            }
            catch (final Throwable err) {
                t = HuterUtil.merge(t, err);
            }

        this.client = null;
        this.hiveServer2 = null;
        this.sessionHandle = null;
        this.currentSessionState = null;

        if (t instanceof Exception)
            throw (Exception) t;
        if (t != null)
            throw new Exception(t);
    }

    private static List<String> splitSemiColon(String line) {
        boolean insideSingleQuote = false;
        boolean insideDoubleQuote = false;
        boolean escape = false;
        int beginIndex = 0;
        List<String> ret = new ArrayList<>();
        for (int index = 0; index < line.length(); index++) {
            if (line.charAt(index) == '\'') {
                // take a look to see if it is escaped
                if (!escape) {
                    // flip the boolean variable
                    insideSingleQuote = !insideSingleQuote;
                }
            }
            else if (line.charAt(index) == '\"') {
                // take a look to see if it is escaped
                if (!escape) {
                    // flip the boolean variable
                    insideDoubleQuote = !insideDoubleQuote;
                }
            }
            else if (line.charAt(index) == ';') {
                if (!insideSingleQuote && !insideDoubleQuote) {
                    // split, do not include ; itself
                    ret.add(line.substring(beginIndex, index));
                    beginIndex = index + 1;
                }
                // else {
                // do not split
                // }
            }
            // else {
            // nothing to do
            // }
            // set the escape
            if (escape) {
                escape = false;
            }
            else if (line.charAt(index) == '\\') {
                escape = true;
            }
        }
        ret.add(line.substring(beginIndex));

        return ret.stream()
                  .map(String::trim)
                  .map(it -> it.endsWith(";") ? it.substring(0, it.length() - 1) : it)
                  .map(it -> Arrays.stream(it.split("\n"))
                                   .filter(it0 -> !it0.trim().startsWith("--"))
                                   .collect(Collectors.joining("\n"))
                                   .trim())
                  .filter(it -> !it.isEmpty())
                  .collect(freezer());
    }

}
