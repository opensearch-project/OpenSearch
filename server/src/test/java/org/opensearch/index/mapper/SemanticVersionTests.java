/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.mapper;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Test class for SemanticVersion
 */
public class SemanticVersionTests extends OpenSearchTestCase {

    public void testBasicVersionParsing() {
        SemanticVersion version = SemanticVersion.parse("1.2.3");
        assertEquals("1.2.3", version.toString());
    }

    public void testPreReleaseVersionParsing() {
        SemanticVersion version = SemanticVersion.parse("1.2.3-alpha");
        assertEquals("1.2.3-alpha", version.toString());

        version = SemanticVersion.parse("1.2.3-alpha.1");
        assertEquals("1.2.3-alpha.1", version.toString());

        version = SemanticVersion.parse("1.2.3-0.3.7");
        assertEquals("1.2.3-0.3.7", version.toString());

        version = SemanticVersion.parse("1.2.3-x.7.z.92");
        assertEquals("1.2.3-x.7.z.92", version.toString());
    }

    public void testBuildMetadataParsing() {
        SemanticVersion version = SemanticVersion.parse("1.2.3+build.123");
        assertEquals("1.2.3+build.123", version.toString());

        version = SemanticVersion.parse("1.2.3+build.123.xyz");
        assertEquals("1.2.3+build.123.xyz", version.toString());
    }

    public void testCompleteVersionParsing() {
        SemanticVersion version = SemanticVersion.parse("1.2.3-alpha.1+build.123");
        assertEquals("1.2.3-alpha.1+build.123", version.toString());
    }

    public void testInvalidVersions() {
        // Test null
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SemanticVersion.parse(null));
        assertEquals("Version string cannot be null or empty", e.getMessage());

        // Test empty string
        e = expectThrows(IllegalArgumentException.class, () -> SemanticVersion.parse(""));
        assertEquals("Version string cannot be null or empty", e.getMessage());

        // Test invalid formats
        List<String> invalidVersions = Arrays.asList(
            "1",
            "1.2",
            "1.2.3.4",
            "1.2.3-",
            "1.2.3+",
            "01.2.3",
            "1.02.3",
            "1.2.03",
            "1.2.3-@invalid",
            "1.2.3+@invalid",
            "a.b.c",
            "-1.2.3",
            "1.-2.3",
            "1.2.-3"
        );

        for (String invalid : invalidVersions) {
            e = expectThrows(IllegalArgumentException.class, () -> SemanticVersion.parse(invalid));
            assertTrue(e.getMessage().contains("Invalid semantic version format"));
        }
    }

    public void testNegativeNumbers() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SemanticVersion(-1, 0, 0, null, null));
        assertEquals("Version numbers cannot be negative", e.getMessage());
    }

    public void testVersionComparison() {
        // Test major version comparison
        assertThat(SemanticVersion.parse("2.0.0").compareTo(SemanticVersion.parse("1.0.0")), greaterThan(0));
        assertThat(SemanticVersion.parse("1.0.0").compareTo(SemanticVersion.parse("2.0.0")), lessThan(0));

        // Test minor version comparison
        assertThat(SemanticVersion.parse("1.2.0").compareTo(SemanticVersion.parse("1.1.0")), greaterThan(0));

        // Test patch version comparison
        assertThat(SemanticVersion.parse("1.1.2").compareTo(SemanticVersion.parse("1.1.1")), greaterThan(0));
    }

    public void testPreReleaseComparison() {
        // Pre-release versions have lower precedence
        assertThat(SemanticVersion.parse("1.0.0-alpha").compareTo(SemanticVersion.parse("1.0.0")), lessThan(0));

        // Numeric identifiers
        assertThat(SemanticVersion.parse("1.0.0-alpha.1").compareTo(SemanticVersion.parse("1.0.0-alpha.2")), lessThan(0));

        // Alphanumeric identifiers
        assertThat(SemanticVersion.parse("1.0.0-alpha").compareTo(SemanticVersion.parse("1.0.0-beta")), lessThan(0));
    }

    public void testComplexPreReleaseComparison() {
        List<String> orderedVersions = Arrays.asList(
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-alpha.beta",
            "1.0.0-beta",
            "1.0.0-beta.2",
            "1.0.0-beta.11",
            "1.0.0-rc.1",
            "1.0.0"
        );

        for (int i = 0; i < orderedVersions.size() - 1; i++) {
            SemanticVersion v1 = SemanticVersion.parse(orderedVersions.get(i));
            SemanticVersion v2 = SemanticVersion.parse(orderedVersions.get(i + 1));
            assertThat(v1.compareTo(v2), lessThan(0));
        }
    }

    public void testBuildMetadataComparison() {
        // Build metadata should be ignored in precedence
        assertEquals(0, SemanticVersion.parse("1.0.0+build.1").compareTo(SemanticVersion.parse("1.0.0+build.2")));
        assertEquals(0, SemanticVersion.parse("1.0.0").compareTo(SemanticVersion.parse("1.0.0+build")));
    }

    public void testNormalizedString() {
        SemanticVersion version = SemanticVersion.parse("1.2.3-alpha+build.123");
        String normalized = version.getNormalizedString();

        // Check padding
        assertTrue(normalized.startsWith("00000000000000000001"));
        assertTrue(normalized.contains("00000000000000000002"));
        assertTrue(normalized.contains("00000000000000000003"));

        // Check pre-release and build metadata
        assertTrue(normalized.contains("-alpha"));
        assertTrue(normalized.contains("+build.123"));
    }

    public void testEdgeCases() {
        // Very large version numbers
        SemanticVersion version = SemanticVersion.parse("999999999.999999999.999999999");
        assertEquals("999999999.999999999.999999999", version.toString());

        // Long pre-release string
        version = SemanticVersion.parse("1.0.0-alpha.beta.gamma.delta.epsilon");
        assertEquals("1.0.0-alpha.beta.gamma.delta.epsilon", version.toString());

        // Long build metadata
        version = SemanticVersion.parse("1.0.0+build.123.456.789.abc.def");
        assertEquals("1.0.0+build.123.456.789.abc.def", version.toString());
    }

    public void testNullComparison() {
        SemanticVersion version = SemanticVersion.parse("1.0.0");
        assertThat(version.compareTo(null), greaterThan(0));
    }

    public void testPreReleaseIdentifierComparison() {
        // Numeric identifiers have lower precedence than non-numeric
        assertThat(SemanticVersion.parse("1.0.0-1").compareTo(SemanticVersion.parse("1.0.0-alpha")), lessThan(0));

        // Longer pre-release version has higher precedence
        assertThat(SemanticVersion.parse("1.0.0-alpha").compareTo(SemanticVersion.parse("1.0.0-alpha.1")), lessThan(0));
    }

    public void testGetNormalizedComparableString() {
        // Stable release - should end with '~'
        SemanticVersion stable = SemanticVersion.parse("1.0.0");
        String stableNorm = stable.getNormalizedComparableString();
        assertTrue(stableNorm.startsWith("00000000000000000001.00000000000000000000.00000000000000000000"));
        assertTrue(stableNorm.endsWith("~"));

        // Pre-release alpha - should end with '-alpha' (lowercase)
        SemanticVersion alpha = SemanticVersion.parse("1.0.0-alpha");
        String alphaNorm = alpha.getNormalizedComparableString();
        assertTrue(alphaNorm.startsWith("00000000000000000001.00000000000000000000.00000000000000000000"));
        assertTrue(alphaNorm.endsWith("-alpha"));

        // Pre-release beta - should end with '-beta'
        SemanticVersion beta = SemanticVersion.parse("1.0.0-beta");
        String betaNorm = beta.getNormalizedComparableString();
        assertTrue(betaNorm.startsWith("00000000000000000001.00000000000000000000.00000000000000000000"));
        assertTrue(betaNorm.endsWith("-beta"));

        // Pre-release with uppercase (should be lowercased in normalized)
        SemanticVersion preReleaseCaps = SemanticVersion.parse("1.0.0-ALPHA.BETA");
        String preReleaseCapsNorm = preReleaseCaps.getNormalizedComparableString();
        assertTrue(preReleaseCapsNorm.endsWith("-alpha.beta"));

        // Stable release with build metadata (build metadata ignored in normalized string)
        SemanticVersion stableWithBuild = SemanticVersion.parse("1.0.0+build.123");
        String stableWithBuildNorm = stableWithBuild.getNormalizedComparableString();
        assertEquals(stableNorm, stableWithBuildNorm);

        // Pre-release with build metadata (build metadata ignored)
        SemanticVersion preReleaseWithBuild = SemanticVersion.parse("1.0.0-beta+build.456");
        String preReleaseWithBuildNorm = preReleaseWithBuild.getNormalizedComparableString();
        assertEquals(betaNorm, preReleaseWithBuildNorm);
    }
}
