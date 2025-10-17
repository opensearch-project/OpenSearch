/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a semantic version number (major.minor.patch-preRelease+build).
 * This class implements semantic versioning (SemVer) according to the specification at semver.org.
 * It provides methods to parse, compare, and manipulate semantic version numbers.
 * Primarily used in {@link SemanticVersionFieldMapper} for mapping and sorting purposes.
 *
 * @see <a href="https://semver.org/">Semantic Versioning 2.0.0</a>
 * @see <a href="https://github.com/opensearch-project/OpenSearch/issues/16814">OpenSearch github issue</a>
 */
public class SemanticVersion implements Comparable<SemanticVersion> {

    // Regex used to check SemVer string. Source: https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
    private static final String SEMANTIC_VERSION_REGEX =
        "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$";
    private final int major;
    private final int minor;
    private final int patch;
    private final String preRelease;
    private final String build;

    public SemanticVersion(int major, int minor, int patch, String preRelease, String build) {
        if (major < 0 || minor < 0 || patch < 0) {
            throw new IllegalArgumentException("Version numbers cannot be negative");
        }
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.preRelease = preRelease;
        this.build = build;
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getPatch() {
        return patch;
    }

    public String getPreRelease() {
        return preRelease;
    }

    public String getBuild() {
        return build;
    }

    public static SemanticVersion parse(String version) {
        if (version == null || version.isEmpty()) {
            throw new IllegalArgumentException("Version string cannot be null or empty");
        }

        // Clean up the input string
        version = version.trim();
        version = version.replaceAll("\\[|\\]", ""); // Remove square brackets

        // Handle encoded byte format
        if (version.matches(".*\\s+.*")) {
            version = version.replaceAll("\\s+", ".");
        }

        Pattern pattern = Pattern.compile(SEMANTIC_VERSION_REGEX);

        Matcher matcher = pattern.matcher(version);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid semantic version format: [" + version + "]");
        }

        try {
            return new SemanticVersion(
                Integer.parseInt(matcher.group(1)),
                Integer.parseInt(matcher.group(2)),
                Integer.parseInt(matcher.group(3)),
                matcher.group(4),
                matcher.group(5)
            );
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid version numbers in: " + version, e);
        }
    }

    /**
     * Returns a normalized string representation of the semantic version.
     * This format ensures proper lexicographical ordering of versions.
     * The format is:
     * - Major, minor, and patch numbers are padded to 20 digits
     * - Pre-release version is appended with a "-" prefix if present
     * - Build metadata is appended with a "+" prefix if present
     *
     * Example: "1.2.3-alpha+build.123" becomes "00000000000000000001.00000000000000000002.00000000000000000003-alpha+build.123"
     *
     * Note: Build metadata is included for completeness but does not affect version precedence
     * as per SemVer specification.
     *
     * @return normalized string representation of the version
     */
    public String getNormalizedString() {
        StringBuilder sb = new StringBuilder();

        // Pad numbers to 20 digits for consistent lexicographical sorting
        // This allows for very large version numbers while maintaining proper order
        sb.append(padWithZeros(major, 20)).append('.').append(padWithZeros(minor, 20)).append('.').append(padWithZeros(patch, 20));

        // Add pre-release version if present
        // Pre-release versions have lower precedence than the associated normal version
        if (preRelease != null) {
            sb.append('-').append(preRelease);
        }

        // Add build metadata if present
        // Note: Build metadata does not affect version precedence
        if (build != null) {
            sb.append('+').append(build);
        }

        return sb.toString();
    }

    /**
     * Returns a normalized comparable string representation of the semantic version.
     * <p>
     * The format zero-pads major, minor, and patch versions to 20 digits each,
     * separated by dots, to ensure correct lexical sorting of numeric components.
     * <p>
     * For pre-release versions, the pre-release label is appended with a leading
     * hyphen (`-`) in lowercase, preserving lexicographical order among pre-release
     * versions.
     * <p>
     * For stable releases (no pre-release), a tilde character (`~`) is appended,
     * which lexically sorts after any pre-release versions to ensure stable
     * releases are ordered last.
     * <p>
     * Ordering: 1.0.0-alpha &lt; 1.0.0-beta &lt; 1.0.0
     * <p>
     * Examples:
     * <ul>
     *   <li>1.0.0      → 00000000000000000001.00000000000000000000.00000000000000000000~</li>
     *   <li>1.0.0-alpha → 00000000000000000001.00000000000000000000.00000000000000000000-alpha</li>
     *   <li>1.0.0-beta  → 00000000000000000001.00000000000000000000.00000000000000000000-beta</li>
     * </ul>
     *
     * @return normalized string for lexicographical comparison of semantic versions
     */
    public String getNormalizedComparableString() {
        StringBuilder sb = new StringBuilder();

        // Zero-pad major, minor, patch
        sb.append(padWithZeros(major, 20)).append(".");
        sb.append(padWithZeros(minor, 20)).append(".");
        sb.append(padWithZeros(patch, 20));

        if (preRelease == null || preRelease.isEmpty()) {
            // Stable release: append '~' to sort AFTER any pre-release
            sb.append("~");
        } else {
            // Pre-release: append '-' plus normalized pre-release string (lowercase, trimmed)
            sb.append("-").append(preRelease.trim().toLowerCase(Locale.ROOT));
        }

        return sb.toString();
    }

    @Override
    public int compareTo(SemanticVersion other) {
        if (other == null) {
            return 1;
        }

        int majorComparison = Integer.compare(this.major, other.major);
        if (majorComparison != 0) return majorComparison;

        int minorComparison = Integer.compare(this.minor, other.minor);
        if (minorComparison != 0) return minorComparison;

        int patchComparison = Integer.compare(this.patch, other.patch);
        if (patchComparison != 0) return patchComparison;

        // Pre-release versions have lower precedence
        if (this.preRelease == null && other.preRelease != null) return 1;
        if (this.preRelease != null && other.preRelease == null) return -1;
        if (this.preRelease != null && other.preRelease != null) {
            return comparePreRelease(this.preRelease, other.preRelease);
        }

        return 0;
    }

    private int comparePreRelease(String pre1, String pre2) {
        String[] parts1 = pre1.split("\\.");
        String[] parts2 = pre2.split("\\.");

        int length = Math.min(parts1.length, parts2.length);
        for (int i = 0; i < length; i++) {
            String part1 = parts1[i];
            String part2 = parts2[i];

            boolean isNum1 = part1.matches("\\d+");
            boolean isNum2 = part2.matches("\\d+");

            if (isNum1 && isNum2) {
                int num1 = Integer.parseInt(part1);
                int num2 = Integer.parseInt(part2);
                int comparison = Integer.compare(num1, num2);
                if (comparison != 0) return comparison;
            } else {
                int comparison = part1.compareTo(part2);
                if (comparison != 0) return comparison;
            }
        }

        return Integer.compare(parts1.length, parts2.length);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(patch);
        if (preRelease != null) {
            sb.append('-').append(preRelease);
        }
        if (build != null) {
            sb.append('+').append(build);
        }
        return sb.toString();
    }

    private static String padWithZeros(long value, int width) {
        String str = Long.toString(value);
        int padding = width - str.length();
        if (padding > 0) {
            char[] zeros = new char[padding];
            Arrays.fill(zeros, '0');
            return new String(zeros) + str;
        }
        return str;
    }

}
