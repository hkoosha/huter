package com.trivago.huter.component;

import com.trivago.huter.runner.HuterContext;
import com.trivago.huter.util.PathToContentFun;

import java.nio.file.Path;

public interface ComponentCreator {

    void create(HuterContext ctx,
                PathToContentFun reader,
                Path dataPath,
                String param) throws Exception;

}
