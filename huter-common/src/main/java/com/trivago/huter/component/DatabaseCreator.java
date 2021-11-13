package com.trivago.huter.component;

import com.trivago.huter.runner.HuterContext;
import com.trivago.huter.util.PathToContentFun;
import org.apache.hive.service.cli.HiveSQLException;

import java.nio.file.Path;
import java.util.List;

final class DatabaseCreator implements ComponentCreator {

    @Override
    public void create(final HuterContext ctx,
                       final PathToContentFun reader,
                       final Path dataPath,
                       final String param) throws HiveSQLException {
        final String sql = "CREATE DATABASE IF NOT EXISTS " + param;
        final List<Object[]> result = ctx.executeSql(sql);
        if (!result.isEmpty())
            throw new HiveSQLException("create database must not return result", sql);
    }

}
