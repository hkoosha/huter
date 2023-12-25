package io.koosha.huter.internal;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.stream.Collectors;

public final class HuterThrowables {

    private HuterThrowables() {
        throw new UnsupportedOperationException("can not instantiate utility class");
    }

    public static String getMessage(final Throwable throwable) {

        return ExceptionUtils
                .getThrowableList(throwable)
                .stream()
                .map(Throwable::getMessage)
                .collect(Collectors.joining(";\n"));
    }

    public static Throwable merge(final Throwable source,
                                  final Throwable other) {
        if (source == null) {
            return other;
        }
        else {
            source.addSuppressed(other);
            return source;
        }
    }

}
