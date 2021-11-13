package com.trivago.huter.component;

import com.trivago.huter.TableLocationFixerHook;
import com.trivago.huter.runner.HuterContext;
import com.trivago.huter.util.PathToContentFun;
import org.apache.hive.service.cli.HiveSQLException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

final class FileBasedTableCreator implements ComponentCreator {

    @Override
    public void create(final HuterContext ctx,
                       final PathToContentFun reader,
                       final Path dataPath,
                       final String param) throws HiveSQLException, IOException {
        final Path loc = dataPath.resolve(param);
        TableLocationFixerHook.prepareAndRememberNextTable(loc);

        final String content = reader.read(Paths.get(param));
        final List<Object[]> result = ctx.executeSql(content);
        if (!result.isEmpty())
            throw new HiveSQLException("create table must not return result", content);
    }

}
