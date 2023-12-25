package io.koosha.huter.internal;

import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class HuterFiles {

    private HuterFiles() {
        throw new UnsupportedOperationException("can not instantiate utility class");
    }

    public static List<Path> subDirectoriesOf(final Path path) throws IOException {

        try (final Stream<Path> paths = Files.list(path)) {
            return paths
                    .filter(Files::isDirectory)
                    .collect(HuterCollections.freezer());
        }
    }

    public static List<Path> subFilesOf(final Path path,
                                        final Predicate<Path> predicate) throws IOException {

        try (Stream<Path> paths = Files.list(path)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(predicate)
                    .collect(HuterCollections.freezer());
        }
    }

    public static void deleteDir(final Path dir) throws IOException {

        List<Path> list;
        try (final Stream<Path> paths = Files.list(dir)) {
            list = paths.collect(Collectors.toList());
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
            throw new IOException("attempting to delete a regular file through recreateDir(), path=" + path);

        if (Files.exists(path))
            deleteDir(path);

        ensureDirectories(path);
    }

    public static void ensureDirectories(final Path path) throws IOException {

        if (!Files.isDirectory(path))
            Files.createDirectories(path);
    }

    public static List<String> readAllLines(final Path path) throws IOException {

        return HuterCollections.freeze(Files.readAllLines(path, StandardCharsets.UTF_8));
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

        final Path check = Paths.get(path);

        return check.toFile().exists() && check.toFile().isDirectory();
    }
}
