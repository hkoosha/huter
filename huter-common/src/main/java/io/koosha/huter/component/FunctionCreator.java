package io.koosha.huter.component;

import io.koosha.huter.runner.HuterContext;
import io.koosha.huter.util.PathToContentFun;
import org.apache.hive.service.cli.HiveSQLException;

import java.nio.file.Path;
import java.util.List;

final class FunctionCreator implements ComponentCreator {

    @Override
    public void create(final HuterContext ctx,
                       final PathToContentFun reader,
                       final Path dataPath,
                       final String param) throws HiveSQLException {
        final String[] params = param.split(ComponentCreatorHub.COMMAND_SEPARATOR_REGEX, 2);
        if (params.length != 2)
            throw new IllegalArgumentException("invalid function syntax: " + param);

        final String functionName = params[0];
        final String javaClass = params[1];

        final String sql = "CREATE FUNCTION " + functionName + " AS " + "'" + javaClass + "'";
        final List<Object[]> result = ctx.executeSql(sql);
        if (!result.isEmpty())
            throw new HiveSQLException("create function must not return result", sql);
    }

}
