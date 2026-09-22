/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.util;

import org.opensearch.gradle.test.GradleUnitTestCase;

public class ExecutableUtilsTests extends GradleUnitTestCase {

    /**
     * Helper method to set system properties for Unix environment and restore them after test execution.
     */
    private void withUnixSystemProperties(Runnable test) {
        String originalFileSeparator = System.getProperty("file.separator");
        String originalPathSeparator = System.getProperty("path.separator");
        try {
            System.setProperty("path.separator", ":");
            System.setProperty("file.separator", "/");
            test.run();
        } finally {
            System.setProperty("file.separator", originalFileSeparator);
            System.setProperty("path.separator", originalPathSeparator);
        }
    }

    /**
     * Helper method to set system properties for Windows environment and restore them after test execution.
     */
    private void withWindowsSystemProperties(Runnable test) {
        String originalFileSeparator = System.getProperty("file.separator");
        String originalPathSeparator = System.getProperty("path.separator");
        try {
            System.setProperty("path.separator", ";");
            System.setProperty("file.separator", "\\");
            test.run();
        } finally {
            System.setProperty("file.separator", originalFileSeparator);
            System.setProperty("path.separator", originalPathSeparator);
        }
    }

    public void testParsePathStringUnix() {
        withUnixSystemProperties(() -> {
            String pathString = "/usr/bin:/usr/local/bin:/home/user/bin";
            String[] result = ExecutableUtils.parsePathString(pathString);

            assertEquals(3, result.length);
            assertEquals("/usr/bin", result[0]);
            assertEquals("/usr/local/bin", result[1]);
            assertEquals("/home/user/bin", result[2]);
        });
    }

    public void testParsePathStringWindows() {
        withWindowsSystemProperties(() -> {
            String pathString = "C:\\Windows\\System32;C:\\Program Files\\bin;C:\\Users\\user\\bin";
            String[] result = ExecutableUtils.parsePathString(pathString);

            assertEquals(3, result.length);
            assertEquals("C:\\Windows\\System32", result[0]);
            assertEquals("C:\\Program Files\\bin", result[1]);
            assertEquals("C:\\Users\\user\\bin", result[2]);
        });
    }

    public void testParsePathSinglePathSeparator() {
        withUnixSystemProperties(() -> {
            String[] paths = { "/" };
            String pathString = String.join(":", paths);
            String[] result = ExecutableUtils.parsePathString(pathString);

            assertEquals(1, result.length);
            assertEquals("/", result[0]);
        });
    }

    public void testParsePathStringWithEmptyEntries() {
        withUnixSystemProperties(() -> {
            String pathString = "/usr/bin::/usr/local/bin:::/home/user/bin:";
            String[] result = ExecutableUtils.parsePathString(pathString);

            // Empty entries should be filtered out
            assertEquals(3, result.length);
            assertEquals("/usr/bin", result[0]);
            assertEquals("/usr/local/bin", result[1]);
            assertEquals("/home/user/bin", result[2]);
        });
    }

    public void testParsePathStringWithTrailingSlashes() {
        withUnixSystemProperties(() -> {
            String pathString = "/usr/bin/:/usr/local/bin/:/home/user/bin/";
            String[] result = ExecutableUtils.parsePathString(pathString);

            // Trailing slashes should be removed
            assertEquals(3, result.length);
            assertEquals("/usr/bin", result[0]);
            assertEquals("/usr/local/bin", result[1]);
            assertEquals("/home/user/bin", result[2]);
        });
    }

    public void testParsePathStringWindowsWithTrailingBackslashes() {
        withWindowsSystemProperties(() -> {
            String pathString = "C:\\Windows\\System32\\;C:\\Program Files\\bin\\";
            String[] result = ExecutableUtils.parsePathString(pathString);

            assertEquals(2, result.length);
            assertEquals("C:\\Windows\\System32", result[0]);
            assertEquals("C:\\Program Files\\bin", result[1]);
        });
    }

    public void testParsePathStringWithWhitespace() {
        withUnixSystemProperties(() -> {
            String pathString = " /usr/bin : /usr/local/bin : /home/user/bin ";
            String[] result = ExecutableUtils.parsePathString(pathString);

            // Whitespace should be trimmed
            assertEquals(3, result.length);
            assertEquals("/usr/bin", result[0]);
            assertEquals("/usr/local/bin", result[1]);
            assertEquals("/home/user/bin", result[2]);
        });
    }

    public void testParsePathStringEmpty() {
        withUnixSystemProperties(() -> {
            String pathString = "";
            String[] result = ExecutableUtils.parsePathString(pathString);

            assertEquals(0, result.length);
        });
    }

    public void testParsePathStringOnlyDelimiters() {
        withUnixSystemProperties(() -> {
            String pathString = ":::";
            String[] result = ExecutableUtils.parsePathString(pathString);

            assertEquals(0, result.length);
        });
    }

    public void testMergePaths() {
        withUnixSystemProperties(() -> {
            String[] path1 = { "/usr/bin", "/usr/local/bin" };
            String[] path2 = { "/home/user/bin", "/opt/bin" };

            String[] result = ExecutableUtils.mergePaths(path1, path2);

            assertEquals(4, result.length);
            assertEquals("/usr/bin", result[0]);
            assertEquals("/usr/local/bin", result[1]);
            assertEquals("/home/user/bin", result[2]);
            assertEquals("/opt/bin", result[3]);
        });
    }

    public void testMergePathsWithDuplicates() {
        withUnixSystemProperties(() -> {
            String[] path1 = { "/usr/bin", "/usr/local/bin", "/home/user/bin" };
            String[] path2 = { "/usr/local/bin", "/opt/bin", "/usr/bin" };

            String[] result = ExecutableUtils.mergePaths(path1, path2);

            // Duplicates should be removed, order preserved from path1 first
            assertEquals(4, result.length);
            assertEquals("/usr/bin", result[0]);
            assertEquals("/usr/local/bin", result[1]);
            assertEquals("/home/user/bin", result[2]);
            assertEquals("/opt/bin", result[3]);
        });
    }
}
