package io.koosha.huter.runner;

import java.util.List;

public interface HuterRunner extends AutoCloseable {

    List<Object[]> run() throws Exception;

}
