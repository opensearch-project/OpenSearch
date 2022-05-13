/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap.jvm;

import org.opensearch.common.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.Runtime.Version;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Parses the list of JVM versions which should be denied to run Opensearch engine with due to discovered
 * issues or flaws.
 *
 * @opensearch.internal
 */
public class DenyJvmVersionsParser {
    /**
     * Provides the reason for the denial
     *
     * @opensearch.internal
     */
    public interface VersionPredicate extends Predicate<Version> {
        String getReason();
    }

    private static class SingleVersion implements VersionPredicate {
        final private Version version;
        final private String reason;

        public SingleVersion(Version version, String reason) {
            this.version = version;
            this.reason = reason;
        }

        @Override
        public boolean test(Version v) {
            return version.compareTo(v) == 0;
        }

        @Override
        public String getReason() {
            return reason;
        }
    }

    private static class VersionRange implements VersionPredicate {
        private Version lower;
        private boolean lowerIncluded;
        private Version upper;
        private boolean upperIncluded;
        private String reason;

        public VersionRange(@Nullable Version lower, boolean lowerIncluded, @Nullable Version upper, boolean upperIncluded, String reason) {
            this.lower = lower;
            this.lowerIncluded = lowerIncluded;
            this.upper = upper;
            this.upperIncluded = upperIncluded;
            this.reason = reason;
        }

        @Override
        public boolean test(Version v) {
            if (lower != null) {
                int compare = lower.compareTo(v);
                if (compare > 0 || (compare == 0 && lowerIncluded != true)) {
                    return false;
                }
            }

            if (upper != null) {
                int compare = upper.compareTo(v);
                if (compare < 0 || (compare == 0 && upperIncluded != true)) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public String getReason() {
            return reason;
        }
    }

    public static Collection<VersionPredicate> getDeniedJvmVersions() {
        try (
            final InputStreamReader in = new InputStreamReader(
                DenyJvmVersionsParser.class.getResourceAsStream("deny-jvm-versions.txt"),
                StandardCharsets.UTF_8
            )
        ) {
            try (final BufferedReader reader = new BufferedReader(in)) {
                return reader.lines()
                    .map(String::trim)
                    // filter empty lines
                    .filter(line -> line.isEmpty() == false)
                    // filter out all comments
                    .filter(line -> line.startsWith("//") == false)
                    .map(DenyJvmVersionsParser::parse)
                    .collect(Collectors.toList());
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException("Unable to read the list of denied JVM versions", ex);
        }
    }

    /**
     * Parse individual line from the list of denied JVM versions. Some version and version range examples are:
     *  <ul>
     *     <li>11.0.2: "... reason ..."             - version 11.0.2</li>
     *     <li>[11.0.2, 11.0.14): "... reason ..."  - versions 11.0.2.2 (included) to 11.0.14 (not included)</li>
     *     <li>[11.0.2, 11.0.14]: "... reason ..."  - versions 11.0.2 to 11.0.14 (both included)</li>
     *     <li>[11.0.2,): "... reason ..."          - versions 11.0.2 and higher</li>
     *  </ul>
     * @param line line to parse
     * @return version or version range predicate
     */
    static VersionPredicate parse(String line) {
        final String[] parts = Arrays.stream(line.split("[:]", 2)).map(String::trim).toArray(String[]::new);

        if (parts.length != 2) {
            throw new IllegalArgumentException("Unable to parse JVM version or version range: " + line);
        }

        final String versionOrRange = parts[0];
        final String reason = parts[1];

        // dealing with version range here
        if (versionOrRange.startsWith("[") == true || versionOrRange.startsWith("(") == true) {
            if (versionOrRange.endsWith("]") == false && versionOrRange.endsWith(")") == false) {
                throw new IllegalArgumentException("Unable to parse JVM version range: " + versionOrRange);
            }

            final boolean lowerIncluded = versionOrRange.startsWith("[");
            final boolean upperIncluded = versionOrRange.endsWith("]");

            final String[] range = Arrays.stream(versionOrRange.substring(1, versionOrRange.length() - 1).split("[,]", 2))
                .map(String::trim)
                .toArray(String[]::new);

            if (range.length != 2) {
                throw new IllegalArgumentException("Unable to parse JVM version range: " + versionOrRange);
            }

            Version lower = null;
            if (range[0].isEmpty() == false && range[0].equals("*") == false) {
                lower = Version.parse(range[0]);
            }

            Version upper = null;
            if (range[1].isEmpty() == false && range[1].equals("*") == false) {
                upper = Version.parse(range[1]);
            }

            return new VersionRange(lower, lowerIncluded, upper, upperIncluded, reason);
        } else {
            // this is just a single version
            return new SingleVersion(Version.parse(versionOrRange), reason);
        }
    }
}
