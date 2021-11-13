package com.trivago.huter.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public final class DefaultResultValidator implements BiFunction<String, List<Object[]>, List<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultResultValidator.class);

    private static final DefaultResultValidator INSTANCE = new DefaultResultValidator();

    public static DefaultResultValidator getInstance() {
        return INSTANCE;
    }

    private DefaultResultValidator() {
    }

    @Override
    public List<String> apply(final String name,
                              final List<Object[]> results) {

        boolean foundAnyBool = false;
        for (final Object[] result : results) {
            LOG.info(">>>>> testing line: {}", Arrays.toString(result));
            for (final Object o : result) {
                if (Objects.equals(o, false)) {
                    LOG.info("======> TEST FAILED ======> {}", name);
                    return Collections.singletonList("test line has failure, line=" + Arrays.toString(result));
                }
                if (Objects.equals(o, true)) {
                    foundAnyBool = true;
                }
            }
        }

        if (!foundAnyBool)
            LOG.warn("======> TEST INVALID ======> test did not contain any boolean! " +
                            "test results are not reliable!!! {} -> [{}]",
                    name, results.stream().map(Arrays::toString).collect(Collectors.joining(";\n")));
        else
            LOG.info("======> TEST SUCCEEDED ======> {}", name);

        return Collections.emptyList();
    }

}
