/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;
import static org.opensearch.LegacyESVersion.V_6_8_15;
import static org.opensearch.LegacyESVersion.V_7_0_0;
import static org.opensearch.test.VersionUtils.randomLegacyVersion;
import static org.opensearch.VersionTests.isCompatible;

/**
 * tests LegacyESVersion utilities.
 * note: legacy version compatibility is already tested by e predecessor
 */
public class LegacyESVersionTests extends OpenSearchTestCase {

    public void testVersionComparison() {
        assertThat(V_6_8_15.before(V_7_0_0), is(true));
        assertThat(V_6_8_15.before(V_6_8_15), is(false));
        assertThat(V_7_0_0.before(V_6_8_15), is(false));

        assertThat(V_6_8_15.onOrBefore(V_7_0_0), is(true));
        assertThat(V_6_8_15.onOrBefore(V_6_8_15), is(true));
        assertThat(V_7_0_0.onOrBefore(V_6_8_15), is(false));

        assertThat(V_6_8_15.after(V_7_0_0), is(false));
        assertThat(V_6_8_15.after(V_6_8_15), is(false));
        assertThat(V_7_0_0.after(V_6_8_15), is(true));

        assertThat(V_6_8_15.onOrAfter(V_7_0_0), is(false));
        assertThat(V_6_8_15.onOrAfter(V_6_8_15), is(true));
        assertThat(V_7_0_0.onOrAfter(V_6_8_15), is(true));

        assertTrue(LegacyESVersion.fromString("5.0.0-alpha2").onOrAfter(LegacyESVersion.fromString("5.0.0-alpha1")));
        assertTrue(LegacyESVersion.fromString("5.0.0").onOrAfter(LegacyESVersion.fromString("5.0.0-beta2")));
        assertTrue(LegacyESVersion.fromString("5.0.0-rc1").onOrAfter(LegacyESVersion.fromString("5.0.0-beta24")));
        assertTrue(LegacyESVersion.fromString("5.0.0-alpha24").before(LegacyESVersion.fromString("5.0.0-beta0")));

        assertThat(V_6_8_15, is(lessThan(V_7_0_0)));
        assertThat(V_6_8_15.compareTo(V_6_8_15), is(0));
        assertThat(V_7_0_0, is(greaterThan(V_6_8_15)));

        // compare opensearch version to LegacyESVersion
        assertThat(Version.V_1_0_0.compareMajor(LegacyESVersion.V_7_0_0), is(0));
        assertThat(Version.V_1_0_0.compareMajor(LegacyESVersion.fromString("6.3.0")), is(1));
        assertThat(LegacyESVersion.fromString("6.3.0").compareMajor(Version.V_1_0_0), is(-1));
    }

    public void testMin() {
        assertEquals(VersionUtils.getPreviousVersion(), LegacyESVersion.min(Version.CURRENT, VersionUtils.getPreviousVersion()));
        assertEquals(LegacyESVersion.fromString("7.0.1"), LegacyESVersion.min(LegacyESVersion.fromString("7.0.1"), Version.CURRENT));
        Version legacyVersion = VersionUtils.randomLegacyVersion(random());
        Version opensearchVersion = VersionUtils.randomOpenSearchVersion(random());
        assertEquals(legacyVersion, Version.min(opensearchVersion, legacyVersion));
    }

    public void testMax() {
        assertEquals(Version.CURRENT, Version.max(Version.CURRENT, VersionUtils.randomLegacyVersion(random())));
        assertEquals(Version.CURRENT, Version.max(LegacyESVersion.fromString("1.0.1"), Version.CURRENT));
        Version legacyVersion = VersionUtils.randomOpenSearchVersion(random());
        Version opensearchVersion = VersionUtils.randomLegacyVersion(random());
        assertEquals(legacyVersion, Version.max(opensearchVersion, legacyVersion));
    }

    public void testMinimumIndexCompatibilityVersion() {
        assertEquals(LegacyESVersion.fromId(5000099), LegacyESVersion.fromId(6000026).minimumIndexCompatibilityVersion());
        assertEquals(LegacyESVersion.fromId(2000099), LegacyESVersion.fromId(5000099).minimumIndexCompatibilityVersion());
        assertEquals(LegacyESVersion.fromId(2000099), LegacyESVersion.fromId(5010000).minimumIndexCompatibilityVersion());
        assertEquals(LegacyESVersion.fromId(2000099), LegacyESVersion.fromId(5000001).minimumIndexCompatibilityVersion());
    }

    public void testVersionFromString() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            LegacyESVersion version = randomLegacyVersion(random());
            assertThat(LegacyESVersion.fromString(version.toString()), sameInstance(version));
        }
    }

    public void testTooLongVersionFromString() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> LegacyESVersion.fromString("1.0.0.1.3"));
        assertThat(e.getMessage(), containsString("needs to contain major, minor, and revision"));
    }

    public void testTooShortVersionFromString() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> LegacyESVersion.fromString("1.0"));
        assertThat(e.getMessage(), containsString("needs to contain major, minor, and revision"));
    }

    public void testWrongVersionFromString() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> LegacyESVersion.fromString("WRONG.VERSION"));
        assertThat(e.getMessage(), containsString("needs to contain major, minor, and revision"));
    }

    public void testVersionNoPresentInSettings() {
        Exception e = expectThrows(IllegalStateException.class, () -> LegacyESVersion.indexCreated(Settings.builder().build()));
        assertThat(e.getMessage(), containsString("[index.version.created] is not present"));
    }

    public void testIndexCreatedVersion() {
        // an actual index has a IndexMetadata.SETTING_INDEX_UUID
        final LegacyESVersion version = (LegacyESVersion) LegacyESVersion.fromId(6000026);
        assertEquals(
            version,
            LegacyESVersion.indexCreated(
                Settings.builder().put(IndexMetadata.SETTING_INDEX_UUID, "foo").put(IndexMetadata.SETTING_VERSION_CREATED, version).build()
            )
        );
    }

    public void testMinCompatVersion() {
        Version major = LegacyESVersion.fromString("6.8.0");
        assertThat(LegacyESVersion.fromString("1.0.0").minimumCompatibilityVersion(), equalTo(major));
        assertThat(LegacyESVersion.fromString("1.2.0").minimumCompatibilityVersion(), equalTo(major));
        assertThat(LegacyESVersion.fromString("1.3.0").minimumCompatibilityVersion(), equalTo(major));

        Version major5x = LegacyESVersion.fromString("5.0.0");
        assertThat(LegacyESVersion.fromString("5.0.0").minimumCompatibilityVersion(), equalTo(major5x));
        assertThat(LegacyESVersion.fromString("5.2.0").minimumCompatibilityVersion(), equalTo(major5x));
        assertThat(LegacyESVersion.fromString("5.3.0").minimumCompatibilityVersion(), equalTo(major5x));

        Version major56x = LegacyESVersion.fromString("5.6.0");
        assertThat(LegacyESVersion.V_6_5_0.minimumCompatibilityVersion(), equalTo(major56x));
        assertThat(LegacyESVersion.fromString("6.3.1").minimumCompatibilityVersion(), equalTo(major56x));

        // from 7.0 on we are supporting the latest minor of the previous major... this might fail once we add a new version ie. 5.x is
        // released since we need to bump the supported minor in Version#minimumCompatibilityVersion()
        LegacyESVersion lastVersion = LegacyESVersion.V_6_8_0; // TODO: remove this once min compat version is a constant instead of method
        assertEquals(lastVersion.major, LegacyESVersion.V_7_0_0.minimumCompatibilityVersion().major);
        assertEquals(
            "did you miss to bump the minor in Version#minimumCompatibilityVersion()",
            lastVersion.minor,
            LegacyESVersion.V_7_0_0.minimumCompatibilityVersion().minor
        );
        assertEquals(0, LegacyESVersion.V_7_0_0.minimumCompatibilityVersion().revision);
    }

    public void testToString() {
        // with 2.0.beta we lowercase
        assertEquals("2.0.0-beta1", LegacyESVersion.fromString("2.0.0-beta1").toString());
        assertEquals("5.0.0-alpha1", LegacyESVersion.fromId(5000001).toString());
        assertEquals("2.3.0", LegacyESVersion.fromString("2.3.0").toString());
        assertEquals("0.90.0.Beta1", LegacyESVersion.fromString("0.90.0.Beta1").toString());
        assertEquals("1.0.0.Beta1", LegacyESVersion.fromString("1.0.0.Beta1").toString());
        assertEquals("2.0.0-beta1", LegacyESVersion.fromString("2.0.0-beta1").toString());
        assertEquals("5.0.0-beta1", LegacyESVersion.fromString("5.0.0-beta1").toString());
        assertEquals("5.0.0-alpha1", LegacyESVersion.fromString("5.0.0-alpha1").toString());
    }

    public void testIsRc() {
        assertTrue(LegacyESVersion.fromString("2.0.0-rc1").isRC());
        assertTrue(LegacyESVersion.fromString("1.0.0.RC1").isRC());

        for (int i = 0; i < 25; i++) {
            assertEquals(LegacyESVersion.fromString("5.0.0-rc" + i).id, LegacyESVersion.fromId(5000000 + i + 50).id);
            assertEquals("5.0.0-rc" + i, LegacyESVersion.fromId(5000000 + i + 50).toString());

            // legacy RC versioning
            assertEquals(LegacyESVersion.fromString("1.0.0.RC" + i).id, LegacyESVersion.fromId(1000000 + i + 50).id);
            assertEquals("1.0.0.RC" + i, LegacyESVersion.fromId(1000000 + i + 50).toString());
        }
    }

    public void testIsBeta() {
        assertTrue(LegacyESVersion.fromString("2.0.0-beta1").isBeta());
        assertTrue(LegacyESVersion.fromString("1.0.0.Beta1").isBeta());
        assertTrue(LegacyESVersion.fromString("0.90.0.Beta1").isBeta());

        for (int i = 0; i < 25; i++) {
            assertEquals(LegacyESVersion.fromString("5.0.0-beta" + i).id, LegacyESVersion.fromId(5000000 + i + 25).id);
            assertEquals("5.0.0-beta" + i, LegacyESVersion.fromId(5000000 + i + 25).toString());
        }
    }

    public void testIsAlpha() {
        assertTrue(new LegacyESVersion(5000001, org.apache.lucene.util.Version.LUCENE_7_0_0).isAlpha());
        assertFalse(new LegacyESVersion(4000002, org.apache.lucene.util.Version.LUCENE_7_0_0).isAlpha());
        assertTrue(new LegacyESVersion(4000002, org.apache.lucene.util.Version.LUCENE_7_0_0).isBeta());
        assertTrue(LegacyESVersion.fromString("5.0.0-alpha14").isAlpha());
        assertEquals(5000014, LegacyESVersion.fromString("5.0.0-alpha14").id);
        assertTrue(LegacyESVersion.fromId(5000015).isAlpha());

        for (int i = 0; i < 25; i++) {
            assertEquals(LegacyESVersion.fromString("5.0.0-alpha" + i).id, LegacyESVersion.fromId(5000000 + i).id);
            assertEquals("5.0.0-alpha" + i, LegacyESVersion.fromId(5000000 + i).toString());
        }
    }

    public void testParseVersion() {
        final int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            LegacyESVersion version = randomLegacyVersion(random());
            LegacyESVersion parsedVersion = (LegacyESVersion) LegacyESVersion.fromString(version.toString());
            assertEquals(version, parsedVersion);
        }

        expectThrows(IllegalArgumentException.class, () -> { LegacyESVersion.fromString("5.0.0-alph2"); });
        assertEquals(LegacyESVersion.fromString("2.0.0-SNAPSHOT"), LegacyESVersion.fromId(2000099));
        expectThrows(IllegalArgumentException.class, () -> { LegacyESVersion.fromString("5.0.0-SNAPSHOT"); });
    }

    public void testAllVersionsMatchId() throws Exception {
        final Set<Version> releasedVersions = new HashSet<>(VersionUtils.allReleasedVersions());
        final Set<Version> unreleasedVersions = new HashSet<>(VersionUtils.allUnreleasedVersions());
        Map<String, Version> maxBranchVersions = new HashMap<>();
        for (java.lang.reflect.Field field : Version.class.getFields()) {
            if (field.getName().matches("_ID")) {
                assertTrue(field.getName() + " should be static", Modifier.isStatic(field.getModifiers()));
                assertTrue(field.getName() + " should be final", Modifier.isFinal(field.getModifiers()));
                int versionId = (Integer) field.get(Version.class);

                String constantName = field.getName().substring(0, field.getName().indexOf("_ID"));
                java.lang.reflect.Field versionConstant = Version.class.getField(constantName);
                assertTrue(constantName + " should be static", Modifier.isStatic(versionConstant.getModifiers()));
                assertTrue(constantName + " should be final", Modifier.isFinal(versionConstant.getModifiers()));

                Version v = (Version) versionConstant.get(null);
                logger.debug("Checking {}", v);
                if (field.getName().endsWith("_UNRELEASED")) {
                    assertTrue(unreleasedVersions.contains(v));
                } else {
                    assertTrue(releasedVersions.contains(v));
                }
                assertEquals("Version id " + field.getName() + " does not point to " + constantName, v, Version.fromId(versionId));
                assertEquals("Version " + constantName + " does not have correct id", versionId, v.id);
                if (v.major >= 2) {
                    String number = v.toString();
                    if (v.isBeta()) {
                        number = number.replace("-beta", "_beta");
                    } else if (v.isRC()) {
                        number = number.replace("-rc", "_rc");
                    } else if (v.isAlpha()) {
                        number = number.replace("-alpha", "_alpha");
                    }
                    assertEquals("V_" + number.replace('.', '_'), constantName);
                } else {
                    assertEquals("V_" + v.toString().replace('.', '_'), constantName);
                }

                // only the latest version for a branch should be a snapshot (ie unreleased)
                String branchName = "" + v.major + "." + v.minor;
                Version maxBranchVersion = maxBranchVersions.get(branchName);
                if (maxBranchVersion == null) {
                    maxBranchVersions.put(branchName, v);
                } else if (v.after(maxBranchVersion)) {
                    if (v == Version.CURRENT) {
                        // Current is weird - it counts as released even though it shouldn't.
                        continue;
                    }
                    assertFalse(
                        "Version " + maxBranchVersion + " cannot be a snapshot because version " + v + " exists",
                        VersionUtils.allUnreleasedVersions().contains(maxBranchVersion)
                    );
                    maxBranchVersions.put(branchName, v);
                }
            }
        }
    }

    public void testIsCompatible() {
        assertTrue(isCompatible(LegacyESVersion.V_6_8_0, LegacyESVersion.V_7_0_0));
        assertFalse(isCompatible(LegacyESVersion.V_6_6_0, LegacyESVersion.V_7_0_0));
        assertFalse(isCompatible(LegacyESVersion.V_6_7_0, LegacyESVersion.V_7_0_0));

        assertFalse(isCompatible(LegacyESVersion.fromId(5000099), LegacyESVersion.fromString("6.0.0")));
        assertFalse(isCompatible(LegacyESVersion.fromId(5000099), LegacyESVersion.fromString("7.0.0")));

        Version a = randomLegacyVersion(random());
        Version b = randomLegacyVersion(random());
        assertThat(a.isCompatible(b), equalTo(b.isCompatible(a)));
    }
}
