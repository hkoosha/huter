package com.trivago.huter.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public final class HuterUtil {

    private HuterUtil() {
        throw new UnsupportedOperationException("can not instantiate utility class");
    }

    // ============================================================= FILES UTIL

    public static List<Path> subDirectoriesOf(final Path path) throws IOException {
        return Files.list(path)
                    .filter(Files::isDirectory)
                    .collect(freezer());
    }

    public static List<Path> subFilesOf(final Path path,
                                        final Predicate<Path> predicate) throws IOException {
        return Files.list(path)
                    .filter(Files::isRegularFile)
                    .filter(predicate)
                    .collect(freezer());
    }


    public static void deleteDir(final Path dir) throws IOException {
        List<Path> list;
        try {
            list = Files.list(dir).collect(Collectors.toList());
        }
        catch (final NoSuchFileException ignore) {
            list = Collections.emptyList();
        }

        for (final Path toDelete : list) {
            if (Files.isDirectory(toDelete)) {
                deleteDir(toDelete);
            }
            else {
                try {
                    Files.delete(toDelete);
                }
                catch (final NoSuchFileException ignore) {
                }
            }
        }

        try {
            Files.delete(dir);
        }
        catch (final NoSuchFileException ignore) {
        }
    }

    public static void recreateDir(final Path path) throws IOException {
        if (Files.isRegularFile(path))
            throw new IOException("attempting to delete a regular file through recreateDir(), location=" + path);

        if (Files.exists(path))
            deleteDir(path);

        ensureDirectories(path);
    }

    public static void ensureDirectories(final Path path) throws IOException {
        if (!Files.isDirectory(path))
            Files.createDirectories(path);
    }


    public static List<String> readAllLines(final Path path) throws IOException {
        return freeze(Files.readAllLines(path, StandardCharsets.UTF_8));
    }

    public static String readFile(final Path path) throws IOException {
        return String.join("\n", readAllLines(path));
    }

    public static void appendToFile(final StringOutputStream sos,
                                    final Path path,
                                    final String name) throws IOException {
        assertIsAbsolute(path);

        final Path dest = path.resolve(name);
        ensureDirectories(path);

        String old;
        try {
            old = FileUtils.readFileToString(dest.toFile(), StandardCharsets.UTF_8);
        }
        catch (final IOException error) {
            old = "";
        }

        FileUtils.write(dest.toFile(), old + "\n\n\n" + sos.toString(), sos.encoding());
    }

    public static void assertIsAbsolute(final Path path) throws IOException {
        if (!path.isAbsolute())
            throw new IOException("path is not absolute: " + path);
    }

    public static boolean isDir(final String path) {
        return Paths.get(path).toFile().exists()
                && Paths.get(path).toFile().isDirectory();
    }


    // ============================================================ STRING UTIL
    public static List<String> filter(final Collection<String> source) {
        return source.stream()
                     .map(String::trim)
                     .filter(it -> !it.isEmpty())
                     .collect(freezer());
    }


    // ============================================================= EXCEPTIONS
    public static String getMessage(final Throwable throwable) {
        return ExceptionUtils.getThrowableList(throwable)
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
