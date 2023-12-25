package io.koosha.huter.runner;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Some configs stolen from Klarna HiveRunner.
 */
final class DefaultRunnerConfigurator {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRunner.class);

    private DefaultRunnerConfigurator() {
        throw new IllegalStateException("can not instantiate utility class");
    }


    static void configureHive(final HuterContext ctx,
                              final HiveConf hc) {
        hc.setVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION, "false");
        hc.setVar(HiveConf.ConfVars.METASTORE_FASTPATH, "true");
        hc.set("javax.jdo.option.ConnectionURL", ctx.getConnectionStr());
        hc.set("hive.support.sql11.reserved.keywords", "false");
        hc.setVar(HiveConf.ConfVars.HIVEVARIABLESUBSTITUTE, "true");
        hc.setVar(HiveConf.ConfVars.HIVEVARIABLESUBSTITUTEDEPTH, "5");

        hc.setVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, "-1");
        hc.setVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, "false");

        hc.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
        hc.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEZ_TEST, true);

        setProp(hc, "metastore.schema.verification", "false");
        setProp(hc, "datanucleus.schema.autoCreateAll", "true");
        setProp(hc, "datanucleus.schema.autoCreateTables", "true");
        setProp(hc, "datanucleus.autoCreateSchema", "true");
        setProp(hc, "metastore.filter.hook", "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl");
        setProp(hc, HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, ctx.getConnectionStr());
    }

    static void configureHiveExperimentalOptions(final HiveConf hc) {
        // setMetastoreProperty(hiveConf, "metastore.filter.hook", DefaultMetaStoreFilterHookImpl.class.getName());
        // setMetastoreProperty(hiveConf, "datanucleus.connectiondrivername", jdbcDriver);
        // setMetastoreProperty(hiveConf, "javax.jdo.option.ConnectionDriverName", jdbcDriver);

        // No pooling needed. This will save us a lot of threads
        hc.set("datanucleus.connectionPoolingType", "None");
        System.setProperty("datanucleus.connectionPoolingType", "None");

        hc.setBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER, false);
        hc.setBoolVar(HiveConf.ConfVars.HIVE_CBO_ENABLED, false);
        hc.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);
        hc.setVar(HiveConf.ConfVars.HADOOPBIN, "NO_BIN!");

        // Switch off all optimizers otherwise we didn't manage to contain the map reduction within this JVM.
        hc.setBoolVar(HiveConf.ConfVars.HIVE_INFER_BUCKET_SORT, false);
        hc.setBoolVar(HiveConf.ConfVars.HIVEMETADATAONLYQUERIES, false);
        hc.setBoolVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER, false);
        hc.setBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN, false);
        hc.setBoolVar(HiveConf.ConfVars.HIVESKEWJOIN, false);

        // Defaults to a 1000 millis sleep in. We can speed up the tests a bit by setting this to 1 millis instead.
        // org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper.
        hc.setLongVar(HiveConf.ConfVars.HIVECOUNTERSPULLINTERVAL, 1L);

        hc.setBoolVar(HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN, true);

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");
        System.setProperty("java.security.krb5.conf", "/dev/null");

        //noinspection SpellCheckingInspection
        ClassLoader
            .getSystemClassLoader()
            .setPackageAssertionStatus("org.apache.hadoop.hive.serde2.objectinspector", false);

        hc.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    }

    static void configureFs(final HiveConf hc, final HuterContext ctx) {
        hc.set("fs.defaultFS", "file:///");
        hc.set("fs.default.name", "file:///");
        hc.set(HiveConf.ConfVars.HIVE_JAR_DIRECTORY.varname, "file://" + ctx.getHiveJarDir());
        // hc.setVar(HiveConf.ConfVars.HIVE_USER_INSTALL_DIR, "");
        hc.set("_hive.hdfs.session.path", "file://" + ctx.getHiveScratchDir());
        hc.set("_hive.local.session.path", "file://" + ctx.getHiveScratchDir());
        // hc.setVar(HiveConf.ConfVars.SCRATCHDIR, "");
        // hc.setVar(HiveConf.ConfVars.HIVEHISTORYFILELOC, "");
        // hc.set("hadoop.tmp.dir", "");
        // hc.set("test.log.dir", "");
        hc.set(HiveConf.ConfVars.LOCALSCRATCHDIR.varname, "//" + ctx.getHiveScratchDir());
        hc.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, "file://" + ctx.getHiveWareHouseDir());
    }

    static void configureTez(final HiveConf hc) {
        hc.set("hive.tez.container.size", "1024");
        hc.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
        hc.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
        hc.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");

        hc.set(TezConfiguration.TEZ_AM_DISABLE_CLIENT_VERSION_CHECK, "true");
        hc.set(TezConfiguration.TEZ_AM_USE_CONCURRENT_DISPATCHER, "false");
        hc.set(TezConfiguration.TEZ_AM_CONTAINER_REUSE_ENABLED, "false");
        hc.set(TezConfiguration.TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX, "1");
        hc.set(TezConfiguration.TEZ_AM_WEBSERVICE_ENABLE, "false");
        hc.set(TezConfiguration.DAG_RECOVERY_ENABLED, "false");
        hc.set(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, "false");
    }

    static void configureDerby(@SuppressWarnings("unused") final HiveConf hc) {
        final File derbyLogFile;
        try {
            derbyLogFile = File.createTempFile("derby", ".log");
            derbyLogFile.deleteOnExit();
            LOG.debug("derby set to log to " + derbyLogFile.getAbsolutePath());
        }
        catch (final IOException e) {
            throw new UncheckedIOException("Error creating temporary derby log file", e);
        }

        System.setProperty("derby.stream.error.file", derbyLogFile.getAbsolutePath());
    }


    private static void setProp(final HiveConf hc,
                                final String key,
                                final String value) {
        hc.set(key, value);
        System.setProperty(key, value);
    }

}
