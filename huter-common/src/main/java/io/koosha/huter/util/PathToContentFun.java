package io.koosha.huter.util;

import java.io.IOException;
import java.nio.file.Path;

public interface PathToContentFun {

    String read(Path path) throws IOException;

}
