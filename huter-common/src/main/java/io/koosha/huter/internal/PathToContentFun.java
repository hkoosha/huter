package io.koosha.huter.internal;

import java.io.IOException;
import java.nio.file.Path;

public interface PathToContentFun {

    String read(Path path) throws IOException;

}
