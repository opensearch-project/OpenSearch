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

    public void testSquareBracketRemoval() {
        // Test that square brackets are removed during parsing
        SemanticVersion version1 = SemanticVersion.parse("[1.2.3]");
        assertEquals("1.2.3", version1.toString());

        SemanticVersion version2 = SemanticVersion.parse("[1.2.3-alpha]");
        assertEquals("1.2.3-alpha", version2.toString());

        SemanticVersion version3 = SemanticVersion.parse("[1.2.3+build.123]");
        assertEquals("1.2.3+build.123", version3.toString());

        SemanticVersion version4 = SemanticVersion.parse("[1.2.3-alpha+build.123]");
        assertEquals("1.2.3-alpha+build.123", version4.toString());
    }

    public void testWhitespaceHandling() {
        // Test that whitespace is converted to dots during parsing
        SemanticVersion version1 = SemanticVersion.parse("1 2 3");
        assertEquals("1.2.3", version1.toString());

        SemanticVersion version2 = SemanticVersion.parse("1  2  3");
        assertEquals("1.2.3", version2.toString());

        SemanticVersion version3 = SemanticVersion.parse("1\t2\t3");
        assertEquals("1.2.3", version3.toString());

        SemanticVersion version4 = SemanticVersion.parse("1 2 3");
        assertEquals("1.2.3", version4.toString());

        // Test mixed whitespace
        SemanticVersion version5 = SemanticVersion.parse("1 \t 2 \t 3");
        assertEquals("1.2.3", version5.toString());
    }

    public void testNumberFormatExceptionHandling() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SemanticVersion.parse("01.02.03"));
        assertTrue(e.getMessage().contains("Invalid semantic version format"));
    }

    public void testComparePreReleaseMethod() {
        // Test numeric vs non-numeric identifiers
        assertThat(SemanticVersion.parse("1.0.0-1").compareTo(SemanticVersion.parse("1.0.0-alpha")), lessThan(0));
        assertThat(SemanticVersion.parse("1.0.0-alpha").compareTo(SemanticVersion.parse("1.0.0-1")), greaterThan(0));

        // Test numeric comparison
        assertThat(SemanticVersion.parse("1.0.0-1").compareTo(SemanticVersion.parse("1.0.0-2")), lessThan(0));
        assertThat(SemanticVersion.parse("1.0.0-10").compareTo(SemanticVersion.parse("1.0.0-2")), greaterThan(0));

        // Test string comparison
        assertThat(SemanticVersion.parse("1.0.0-alpha").compareTo(SemanticVersion.parse("1.0.0-beta")), lessThan(0));
        assertThat(SemanticVersion.parse("1.0.0-beta").compareTo(SemanticVersion.parse("1.0.0-alpha")), greaterThan(0));

        // Test mixed numeric and string
        assertThat(SemanticVersion.parse("1.0.0-alpha.1").compareTo(SemanticVersion.parse("1.0.0-alpha.2")), lessThan(0));
        assertThat(SemanticVersion.parse("1.0.0-alpha.2").compareTo(SemanticVersion.parse("1.0.0-alpha.10")), lessThan(0));

        // Test different lengths
        assertThat(SemanticVersion.parse("1.0.0-alpha").compareTo(SemanticVersion.parse("1.0.0-alpha.1")), lessThan(0));
        assertThat(SemanticVersion.parse("1.0.0-alpha.1").compareTo(SemanticVersion.parse("1.0.0-alpha")), greaterThan(0));
    }

    public void testPadWithZerosMethod() {
        // Test with small numbers that need padding
        SemanticVersion version1 = SemanticVersion.parse("1.2.3");
        String normalized1 = version1.getNormalizedString();
        assertEquals("00000000000000000001.00000000000000000002.00000000000000000003", normalized1);

        // Test with larger numbers that need less padding
        SemanticVersion version2 = SemanticVersion.parse("123.456.789");
        String normalized2 = version2.getNormalizedString();
        assertEquals("00000000000000000123.00000000000000000456.00000000000000000789", normalized2);

        // Test with very large numbers
        SemanticVersion version3 = SemanticVersion.parse("999999999.999999999.999999999");
        String normalized3 = version3.getNormalizedString();
        assertEquals("00000000000999999999.00000000000999999999.00000000000999999999", normalized3);

        // Test with zero values
        SemanticVersion version4 = SemanticVersion.parse("0.0.0");
        String normalized4 = version4.getNormalizedString();
        assertEquals("00000000000000000000.00000000000000000000.00000000000000000000", normalized4);

        // Test with a value that doesn't need padding
        String str = "9999999999";
        String padded = SemanticVersion.parse("1.0.0").toString();
    }

    /**
     * Test to cover getBuild() method
     */
    public void testGetBuild() {
        // Test with build metadata
        SemanticVersion version = SemanticVersion.parse("1.2.3+build.123");
        assertEquals("build.123", version.getBuild());

        // Test without build metadata
        SemanticVersion versionNoBuild = SemanticVersion.parse("1.2.3");
        assertNull(versionNoBuild.getBuild());
    }

    /**
     * Test to cover NumberFormatException handling in parse method
     */
    public void testNumberFormatExceptionInParse() {
        try {
            // This should throw IllegalArgumentException with a NumberFormatException cause
            SemanticVersion.parse("2147483648.0.0"); // Integer.MAX_VALUE + 1
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            // Verify that the exception message contains the expected text
            assertTrue(e.getMessage().contains("Invalid version numbers"));
            // Verify that the cause is a NumberFormatException
            assertTrue(e.getCause() instanceof NumberFormatException);
        }
    }

    /**
     * Test to cover the else branch in comparePreRelease method
     */
    public void testComparePreReleaseElseBranch() {
        // Create versions with non-numeric pre-release identifiers
        SemanticVersion v1 = SemanticVersion.parse("1.0.0-alpha");
        SemanticVersion v2 = SemanticVersion.parse("1.0.0-beta");

        // alpha comes before beta lexically
        assertThat(v1.compareTo(v2), lessThan(0));
        assertThat(v2.compareTo(v1), greaterThan(0));

        // Test with mixed case to ensure case sensitivity is handled
        SemanticVersion v3 = SemanticVersion.parse("1.0.0-Alpha");
        SemanticVersion v4 = SemanticVersion.parse("1.0.0-alpha");

        // Uppercase comes before lowercase in ASCII
        assertThat(v3.compareTo(v4), lessThan(0));
    }
}
