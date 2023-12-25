package io.koosha.huter.component;

import io.koosha.huter.runner.HuterContext;
import io.koosha.huter.util.PathToContentFun;

import java.nio.file.Path;

public interface ComponentCreator {

    void create(HuterContext ctx,
                PathToContentFun reader,
                Path dataPath,
                String param) throws Exception;

}
