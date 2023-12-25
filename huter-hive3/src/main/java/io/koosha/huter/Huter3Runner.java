package io.koosha.huter;

import io.koosha.huter.main.HuterRepoMain;

public final class Huter3Runner {

    private Huter3Runner() {
        throw new UnsupportedOperationException("can not instantiate utility class");
    }

    public static void main(final String... args) throws Exception {

//        HuterRepoMain.main(args);

        HuterRepoMain.main("/home/milan/git/huter/example/");

    }

}
