/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.util;

import org.apache.tools.ant.taskdefs.condition.Os;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Set of utils for reading and parsing environment PATH variable and for finding executable files.
 */
public class ExecutableUtils {
    private static String[] cachedPathEnv;

    /**
     * Normalizes the path by trimming and removing redundant "/" or "\" at the end of the path.
     * @param path Path string to normalize.
     * @return Normalized path.
     */
    private static String normalizePath(String path) {
        String trimmedPath = path.trim();
        if (trimmedPath.length() > 1 && trimmedPath.endsWith(getFileSeparator())) {
            return trimmedPath.substring(0, trimmedPath.length() - 1);
        }

        return trimmedPath;
    }

    private ExecutableUtils() {}

    private static String getFileSeparator() {
        return System.getProperty("file.separator", Os.isFamily(Os.FAMILY_WINDOWS) ? "\\" : "/");
    }

    private static String getPathSeparator() {
        return System.getProperty("path.separator", Os.isFamily(Os.FAMILY_WINDOWS) ? ";" : ":");
    }

    /**
     * Parses path string into an array of single paths, normalizing them and removing empty paths.
     * @param pathString Path string to parse.
     * @return An array of single paths.
     */
    public static String[] parsePathString(String pathString) {
        String[] pathArray = pathString.split(getPathSeparator());
        return Arrays.stream(pathArray).filter(path -> !path.isEmpty()).map(path -> normalizePath(path)).toArray(String[]::new);

    }

    /**
     * @return Path environment variable in form of array of normalized paths.
     */
    public static String[] getPathEnv() {
        if (cachedPathEnv != null) {
            return cachedPathEnv;
        }

        String pathEnvString = System.getenv("PATH");

        cachedPathEnv = pathEnvString != null ? parsePathString(pathEnvString) : new String[0];
        return cachedPathEnv;
    }

    /**
     * Merges two arrays of paths, removing duplicates and keeping the order. It expects the provided paths to be normalized.
     * @param path1 First array of normalized paths.
     * @param path2 Second array of normalized paths.
     * @return An array of merged paths.
     */
    public static String[] mergePaths(String[] path1, String[] path2) {
        return Stream.concat(Arrays.stream(path1), Arrays.stream(path2))
            // LinkedHashSet removes duplicates and keeps the order
            .collect(Collectors.toCollection(LinkedHashSet::new))
            .toArray(String[]::new);
    }

    /**
     * Finds executable with given filename in paths defined in PATH environment variable.
     * @param executableFileName A filename of executable (with extension included if present).
     * @return An optional path to found executable.
     */
    public static Optional<String> findExecutable(String executableFileName) {
        return findExecutable(executableFileName, getPathEnv());
    }

    /**
     * Find executable with given name in provided paths.
     * @param executableFileName A filename of executable (with extension included if present).
     * @param path Array of paths where to look for executable.
     * @return An optional path to found executable.
     */
    public static Optional<String> findExecutable(String executableFileName, String[] path) {
        return Arrays.stream(path)
            .map(p -> Path.of(p, executableFileName))
            .map(Path::toFile)
            .filter(File::exists)
            .filter(File::canExecute)
            .findFirst()
            .map(File::toString);
    }

    /**
     * Find executable with given name in known path, with fallback to paths defined in PATH environment.
     * @param executableFileName A filename of executable (with extension included if present).
     * @param knownPath Array of paths where to look for executable first, before looking into system PATH.
     * @return An optional path to found executable.
     */
    public static Optional<String> findExecutableInKnownPaths(String executableFileName, String[] knownPath) {
        String[] mergedPath = mergePaths(knownPath, getPathEnv());

        return findExecutable(executableFileName, mergedPath);
    }
}
