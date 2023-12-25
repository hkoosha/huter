package io.koosha.huter.internal;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public final class HuterCollections {

    private HuterCollections() {
        throw new UnsupportedOperationException("can not instantiate utility class");
    }

    // ============================================================ STRINGS

    public static List<String> filter(final Collection<String> source) {

        return source.stream()
                .map(String::trim)
                .filter(it -> !it.isEmpty())
                .collect(freezer());
    }

    // ============================================================= COLLECTIONS

    public static <T> Set<T> freeze(final Set<T> set) {

        return Collections.unmodifiableSet(set);
    }

    public static <T> List<T> freeze(final List<T> list) {

        return Collections.unmodifiableList(list);
    }

    public static <T> Collector<T, ?, List<T>> freezer() {

        return new Freezer<>();
    }

    private static final class Freezer<T> implements Collector<T, List<T>, List<T>> {

        @Override
        public BiConsumer<List<T>, T> accumulator() {
            return List::add;
        }

        @Override
        public Supplier<List<T>> supplier() {
            return ArrayList::new;
        }

        @Override
        public BinaryOperator<List<T>> combiner() {
            return (left, right) -> {
                left.addAll(right);
                return left;
            };
        }

        @Override
        public Function<List<T>, List<T>> finisher() {
            return Collections::unmodifiableList;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }

    }

}
