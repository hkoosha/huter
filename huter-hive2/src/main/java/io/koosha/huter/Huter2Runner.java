package io.koosha.huter;

import io.koosha.huter.main.HuterRepoMain;

public final class Huter2Runner {

    private Huter2Runner() {
        throw new UnsupportedOperationException("can not instantiate utility class");
    }

    public static void main(final String... args) throws Exception {

        HuterRepoMain.main(args);

    }

}
