/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.reindex.remote;

import org.opensearch.test.OpenSearchTestCase;

public class RemoteVersionTests extends OpenSearchTestCase {

    public void testVersionComparison() {
        // Test before()
        assertTrue(RemoteVersion.ELASTICSEARCH_1_0_0.before(RemoteVersion.ELASTICSEARCH_2_0_0));
        assertFalse(RemoteVersion.ELASTICSEARCH_1_0_0.before(RemoteVersion.ELASTICSEARCH_1_0_0));
        assertFalse(RemoteVersion.ELASTICSEARCH_2_0_0.before(RemoteVersion.ELASTICSEARCH_1_0_0));

        // Test onOrBefore()
        assertTrue(RemoteVersion.ELASTICSEARCH_1_0_0.onOrBefore(RemoteVersion.ELASTICSEARCH_2_0_0));
        assertTrue(RemoteVersion.ELASTICSEARCH_1_0_0.onOrBefore(RemoteVersion.ELASTICSEARCH_1_0_0));
        assertFalse(RemoteVersion.ELASTICSEARCH_2_0_0.onOrBefore(RemoteVersion.ELASTICSEARCH_1_0_0));

        // Test after()
        assertFalse(RemoteVersion.ELASTICSEARCH_1_0_0.after(RemoteVersion.ELASTICSEARCH_2_0_0));
        assertFalse(RemoteVersion.ELASTICSEARCH_1_0_0.after(RemoteVersion.ELASTICSEARCH_1_0_0));
        assertTrue(RemoteVersion.ELASTICSEARCH_2_0_0.after(RemoteVersion.ELASTICSEARCH_1_0_0));

        // Test onOrAfter()
        assertFalse(RemoteVersion.ELASTICSEARCH_1_0_0.onOrAfter(RemoteVersion.ELASTICSEARCH_2_0_0));
        assertTrue(RemoteVersion.ELASTICSEARCH_1_0_0.onOrAfter(RemoteVersion.ELASTICSEARCH_1_0_0));
        assertTrue(RemoteVersion.ELASTICSEARCH_2_0_0.onOrAfter(RemoteVersion.ELASTICSEARCH_1_0_0));

        // Test compareTo()
        assertTrue(RemoteVersion.ELASTICSEARCH_1_0_0.compareTo(RemoteVersion.ELASTICSEARCH_2_0_0) < 0);
        assertTrue(RemoteVersion.ELASTICSEARCH_2_0_0.compareTo(RemoteVersion.ELASTICSEARCH_1_0_0) > 0);
        assertTrue(RemoteVersion.ELASTICSEARCH_2_0_0.compareTo(RemoteVersion.ELASTICSEARCH_2_0_0) == 0);

        // Test OpenSearch vs Elasticsearch versions with same numbers
        assertTrue(RemoteVersion.ELASTICSEARCH_2_0_0.compareTo(RemoteVersion.OPENSEARCH_2_0_0) < 0);
        assertTrue(RemoteVersion.OPENSEARCH_2_0_0.compareTo(RemoteVersion.ELASTICSEARCH_2_0_0) > 0);
    }

    public void testVersionFromString() {
        // Test basic version parsing
        RemoteVersion version = RemoteVersion.fromString("7.10.2", false);
        assertEquals(7, version.getMajor());
        assertEquals(10, version.getMinor());
        assertEquals(2, version.getRevision());
        assertFalse(version.isOpenSearch);

        // Test version with snapshot qualifier
        RemoteVersion snapshotVersion = RemoteVersion.fromString("2.0.0-SNAPSHOT", false);
        assertEquals(2, snapshotVersion.getMajor());
        assertEquals(0, snapshotVersion.getMinor());
        assertEquals(0, snapshotVersion.getRevision());

        // Test version with pre-release qualifiers
        RemoteVersion alphaVersion = RemoteVersion.fromString("2.0.0-alpha1", false);
        assertEquals(2, alphaVersion.getMajor());
        assertEquals(0, alphaVersion.getMinor());
        assertEquals(0, alphaVersion.getRevision());

        RemoteVersion betaVersion = RemoteVersion.fromString("2.0.0-beta2", false);
        assertEquals(2, betaVersion.getMajor());
        assertEquals(0, betaVersion.getMinor());
        assertEquals(0, betaVersion.getRevision());

        RemoteVersion rcVersion = RemoteVersion.fromString("2.0.0-rc1", false);
        assertEquals(2, rcVersion.getMajor());
        assertEquals(0, rcVersion.getMinor());
        assertEquals(0, rcVersion.getRevision());
    }

    public void testInvalidVersionFromString() {
        // Test null version
        Exception e = expectThrows(IllegalArgumentException.class, () -> RemoteVersion.fromString(null, false));
        assertTrue(e.getMessage().contains("Version string cannot be null or empty"));

        // Test empty version
        e = expectThrows(IllegalArgumentException.class, () -> RemoteVersion.fromString("", false));
        assertTrue(e.getMessage().contains("Version string cannot be null or empty"));

        // Test invalid format
        e = expectThrows(IllegalArgumentException.class, () -> RemoteVersion.fromString("invalid", false));
        assertTrue(e.getMessage().contains("Invalid version format"));

        // Test non-numeric version
        e = expectThrows(IllegalArgumentException.class, () -> RemoteVersion.fromString("a.b.c", false));
        assertTrue(e.getMessage().contains("Invalid version format"));
    }

    public void testVersionConstants() {
        // Test Elasticsearch versions
        assertEquals(0, RemoteVersion.ELASTICSEARCH_0_20_5.getMajor());
        assertEquals(20, RemoteVersion.ELASTICSEARCH_0_20_5.getMinor());
        assertEquals(5, RemoteVersion.ELASTICSEARCH_0_20_5.getRevision());
        assertFalse(RemoteVersion.ELASTICSEARCH_0_20_5.isOpenSearch);

        assertEquals(7, RemoteVersion.ELASTICSEARCH_7_0_0.getMajor());
        assertEquals(0, RemoteVersion.ELASTICSEARCH_7_0_0.getMinor());
        assertEquals(0, RemoteVersion.ELASTICSEARCH_7_0_0.getRevision());
        assertFalse(RemoteVersion.ELASTICSEARCH_7_0_0.isOpenSearch);

        // Test OpenSearch versions
        assertEquals(1, RemoteVersion.OPENSEARCH_1_0_0.getMajor());
        assertEquals(0, RemoteVersion.OPENSEARCH_1_0_0.getMinor());
        assertEquals(0, RemoteVersion.OPENSEARCH_1_0_0.getRevision());
        assertTrue(RemoteVersion.OPENSEARCH_1_0_0.isOpenSearch);

        assertEquals(2, RemoteVersion.OPENSEARCH_2_0_0.getMajor());
        assertEquals(0, RemoteVersion.OPENSEARCH_2_0_0.getMinor());
        assertEquals(0, RemoteVersion.OPENSEARCH_2_0_0.getRevision());
        assertTrue(RemoteVersion.OPENSEARCH_2_0_0.isOpenSearch);

        assertEquals(3, RemoteVersion.OPENSEARCH_3_1_0.getMajor());
        assertEquals(1, RemoteVersion.OPENSEARCH_3_1_0.getMinor());
        assertEquals(0, RemoteVersion.OPENSEARCH_3_1_0.getRevision());
        assertTrue(RemoteVersion.OPENSEARCH_3_1_0.isOpenSearch);
    }

    public void testVersionOrdering() {
        // Test chronological ordering of constants
        assertTrue(RemoteVersion.ELASTICSEARCH_0_20_5.before(RemoteVersion.ELASTICSEARCH_0_90_13));
        assertTrue(RemoteVersion.ELASTICSEARCH_0_90_13.before(RemoteVersion.ELASTICSEARCH_1_0_0));
        assertTrue(RemoteVersion.ELASTICSEARCH_1_0_0.before(RemoteVersion.ELASTICSEARCH_1_7_5));
        assertTrue(RemoteVersion.ELASTICSEARCH_1_7_5.before(RemoteVersion.ELASTICSEARCH_2_0_0));
        assertTrue(RemoteVersion.ELASTICSEARCH_2_0_0.before(RemoteVersion.ELASTICSEARCH_2_1_0));
        assertTrue(RemoteVersion.ELASTICSEARCH_2_1_0.before(RemoteVersion.ELASTICSEARCH_2_3_3));
        assertTrue(RemoteVersion.ELASTICSEARCH_2_3_3.before(RemoteVersion.ELASTICSEARCH_5_0_0));
        assertTrue(RemoteVersion.ELASTICSEARCH_5_0_0.before(RemoteVersion.ELASTICSEARCH_6_0_0));
        assertTrue(RemoteVersion.ELASTICSEARCH_6_0_0.before(RemoteVersion.ELASTICSEARCH_6_3_0));
        assertTrue(RemoteVersion.ELASTICSEARCH_6_3_0.before(RemoteVersion.ELASTICSEARCH_7_0_0));
    }

    public void testEqualsAndHashCode() {
        RemoteVersion version1 = new RemoteVersion(2, 0, 0, false);
        RemoteVersion version2 = new RemoteVersion(2, 0, 0, false);
        RemoteVersion version3 = new RemoteVersion(2, 0, 0, true);
        RemoteVersion version4 = new RemoteVersion(2, 0, 1, false);

        // Test equals
        assertEquals(version1, version2);
        assertNotEquals(version1, version3); // Different isOpenSearch
        assertNotEquals(version1, version4); // Different revision
        assertNotEquals(version1, null);
        assertNotEquals(version1, "not a version");

        // Test hashCode consistency
        assertEquals(version1.hashCode(), version2.hashCode());
        assertNotEquals(version1.hashCode(), version3.hashCode());

        // Test reflexivity
        assertEquals(version1, version1);
    }

    /**
     * Test equals method
     */
    public void testEqualsMethodAllBranches() {
        RemoteVersion baseVersion = new RemoteVersion(2, 1, 3, false);

        // Test same object (reflexivity)
        assertEquals(baseVersion, baseVersion);

        // Test null
        assertNotEquals(baseVersion, null);

        // Test different class
        assertNotEquals(baseVersion, "not a version object");

        // Test different major version
        RemoteVersion differentMajor = new RemoteVersion(1, 1, 3, false);
        assertNotEquals(baseVersion, differentMajor);

        // Test different minor version
        RemoteVersion differentMinor = new RemoteVersion(2, 0, 3, false);
        assertNotEquals(baseVersion, differentMinor);

        // Test different revision
        RemoteVersion differentRevision = new RemoteVersion(2, 1, 2, false);
        assertNotEquals(baseVersion, differentRevision);

        // Test different isOpenSearch
        RemoteVersion differentIsOpenSearch = new RemoteVersion(2, 1, 3, true);
        assertNotEquals(baseVersion, differentIsOpenSearch);

        // Test identical version (all fields match)
        RemoteVersion identical = new RemoteVersion(2, 1, 3, false);
        assertEquals(baseVersion, identical);
    }

    public void testToString() {
        RemoteVersion version = new RemoteVersion(2, 1, 3, false);
        assertEquals("2.1.3", version.toString());

        RemoteVersion opensearchVersion = new RemoteVersion(1, 0, 0, true);
        assertEquals("1.0.0", opensearchVersion.toString());
    }

    public void testCompareToWithNull() {
        RemoteVersion version = RemoteVersion.ELASTICSEARCH_2_0_0;
        assertTrue(version.compareTo(null) > 0);
    }

    public void testVersionConstantsAreCorrect() {
        // Verify that our constants match what fromString would produce
        assertEquals(RemoteVersion.ELASTICSEARCH_2_0_0, RemoteVersion.fromString("2.0.0", false));
        assertEquals(RemoteVersion.ELASTICSEARCH_7_0_0, RemoteVersion.fromString("7.0.0", false));
        assertEquals(RemoteVersion.OPENSEARCH_1_0_0, RemoteVersion.fromString("1.0.0", true));
        assertEquals(RemoteVersion.OPENSEARCH_2_0_0, RemoteVersion.fromString("2.0.0", true));
    }
}
