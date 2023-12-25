package io.koosha.huter.main;

import io.koosha.huter.runner.HuterContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.koosha.huter.internal.HuterCollections.freeze;
import static io.koosha.huter.internal.HuterCollections.freezer;

// Methods referenced from py4j
@SuppressWarnings("unused")
public final class Result {

    private final List<String> errors;

    private final List<Object[]> output;

    private final String huterOutput;

    public List<String> getErrors() {
        return this.errors;
    }

    public List<Object[]> getOutput() {
        return this.output;
    }

    public String getHuterOutput() {
        return this.huterOutput;
    }

    private Result(final List<String> errors,
                   final List<Object[]> output,
                   final String huterOutput) {

        this.output = output
                .stream()
                .map(Object[]::clone)
                .collect(freezer());
        this.errors = freeze(new ArrayList<>(errors));
        this.huterOutput = huterOutput;
    }

    @Override
    public String toString() {
        return "Result{" +
                "output='" + output.size() + '\'' +
                ", errors='" + errors + '\'' +
                ", huterOutput='" + huterOutput + '\'' +
                '}';
    }


    static Result create(final HuterContext ctx,
                         final List<Object[]> output,
                         final List<String> errors) {

        return new Result(
                errors,
                output,
                ctx == null ? null : ctx.getHuterOutput().toString()
        );
    }

    static Result create(final HuterContext ctx,
                         final String error) {

        return create(
                ctx,
                Collections.emptyList(),
                Collections.singletonList(error)
        );
    }

}
