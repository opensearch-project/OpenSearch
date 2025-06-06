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
        assertTrue(RemoteVersion.V_1_0_0.before(RemoteVersion.V_2_0_0));
        assertFalse(RemoteVersion.V_1_0_0.before(RemoteVersion.V_1_0_0));
        assertFalse(RemoteVersion.V_2_0_0.before(RemoteVersion.V_1_0_0));

        // Test onOrBefore()
        assertTrue(RemoteVersion.V_1_0_0.onOrBefore(RemoteVersion.V_2_0_0));
        assertTrue(RemoteVersion.V_1_0_0.onOrBefore(RemoteVersion.V_1_0_0));
        assertFalse(RemoteVersion.V_2_0_0.onOrBefore(RemoteVersion.V_1_0_0));

        // Test after()
        assertFalse(RemoteVersion.V_1_0_0.after(RemoteVersion.V_2_0_0));
        assertFalse(RemoteVersion.V_1_0_0.after(RemoteVersion.V_1_0_0));
        assertTrue(RemoteVersion.V_2_0_0.after(RemoteVersion.V_1_0_0));

        // Test onOrAfter()
        assertFalse(RemoteVersion.V_1_0_0.onOrAfter(RemoteVersion.V_2_0_0));
        assertTrue(RemoteVersion.V_1_0_0.onOrAfter(RemoteVersion.V_1_0_0));
        assertTrue(RemoteVersion.V_2_0_0.onOrAfter(RemoteVersion.V_1_0_0));

        // Test compareTo()
        assertTrue(RemoteVersion.V_1_0_0.compareTo(RemoteVersion.V_2_0_0) < 0);
        assertTrue(RemoteVersion.V_2_0_0.compareTo(RemoteVersion.V_1_0_0) > 0);

        // Test OpenSearch vs Elasticsearch versions with same numbers
        assertEquals(0, RemoteVersion.V_2_0_0.compareTo(RemoteVersion.OPENSEARCH_2_0_0)); // Same version numbers should be equal in
                                                                                          // comparison
    }

    public void testVersionFromString() {
        // Test basic version parsing
        RemoteVersion version = RemoteVersion.fromString("7.10.2");
        assertEquals(7, version.getMajor());
        assertEquals(10, version.getMinor());
        assertEquals(2, version.getRevision());
        assertEquals("elasticsearch", version.getDistribution());

        // Test version with distribution
        RemoteVersion opensearchVersion = RemoteVersion.fromString("1.0.0", "opensearch");
        assertEquals(1, opensearchVersion.getMajor());
        assertEquals(0, opensearchVersion.getMinor());
        assertEquals(0, opensearchVersion.getRevision());
        assertEquals("opensearch", opensearchVersion.getDistribution());

        // Test version without revision
        RemoteVersion versionNoRevision = RemoteVersion.fromString("2.1");
        assertEquals(2, versionNoRevision.getMajor());
        assertEquals(1, versionNoRevision.getMinor());
        assertEquals(0, versionNoRevision.getRevision());

        // Test version with snapshot qualifier
        RemoteVersion snapshotVersion = RemoteVersion.fromString("2.0.0-SNAPSHOT");
        assertEquals(2, snapshotVersion.getMajor());
        assertEquals(0, snapshotVersion.getMinor());
        assertEquals(0, snapshotVersion.getRevision());

        // Test version with pre-release qualifiers
        RemoteVersion alphaVersion = RemoteVersion.fromString("2.0.0-alpha1");
        assertEquals(2, alphaVersion.getMajor());
        assertEquals(0, alphaVersion.getMinor());
        assertEquals(0, alphaVersion.getRevision());

        RemoteVersion betaVersion = RemoteVersion.fromString("2.0.0-beta2");
        assertEquals(2, betaVersion.getMajor());
        assertEquals(0, betaVersion.getMinor());
        assertEquals(0, betaVersion.getRevision());

        RemoteVersion rcVersion = RemoteVersion.fromString("2.0.0-rc1");
        assertEquals(2, rcVersion.getMajor());
        assertEquals(0, rcVersion.getMinor());
        assertEquals(0, rcVersion.getRevision());
    }

    public void testInvalidVersionFromString() {
        // Test null version
        Exception e = expectThrows(IllegalArgumentException.class, () -> RemoteVersion.fromString(null));
        assertTrue(e.getMessage().contains("Version string cannot be null or empty"));

        // Test empty version
        e = expectThrows(IllegalArgumentException.class, () -> RemoteVersion.fromString(""));
        assertTrue(e.getMessage().contains("Version string cannot be null or empty"));

        // Test invalid format
        e = expectThrows(IllegalArgumentException.class, () -> RemoteVersion.fromString("invalid"));
        assertTrue(e.getMessage().contains("Invalid version format"));

        // Test non-numeric version
        e = expectThrows(IllegalArgumentException.class, () -> RemoteVersion.fromString("a.b.c"));
        assertTrue(e.getMessage().contains("Invalid version format"));
    }

    public void testVersionConstants() {
        // Test Elasticsearch versions
        assertEquals(0, RemoteVersion.V_0_20_5.getMajor());
        assertEquals(20, RemoteVersion.V_0_20_5.getMinor());
        assertEquals(5, RemoteVersion.V_0_20_5.getRevision());
        assertEquals("elasticsearch", RemoteVersion.V_0_20_5.getDistribution());

        assertEquals(7, RemoteVersion.V_7_0_0.getMajor());
        assertEquals(0, RemoteVersion.V_7_0_0.getMinor());
        assertEquals(0, RemoteVersion.V_7_0_0.getRevision());
        assertEquals("elasticsearch", RemoteVersion.V_7_0_0.getDistribution());

        // Test OpenSearch versions
        assertEquals(1, RemoteVersion.OPENSEARCH_1_0_0.getMajor());
        assertEquals(0, RemoteVersion.OPENSEARCH_1_0_0.getMinor());
        assertEquals(0, RemoteVersion.OPENSEARCH_1_0_0.getRevision());
        assertEquals("opensearch", RemoteVersion.OPENSEARCH_1_0_0.getDistribution());

        assertEquals(2, RemoteVersion.OPENSEARCH_2_0_0.getMajor());
        assertEquals(0, RemoteVersion.OPENSEARCH_2_0_0.getMinor());
        assertEquals(0, RemoteVersion.OPENSEARCH_2_0_0.getRevision());
        assertEquals("opensearch", RemoteVersion.OPENSEARCH_2_0_0.getDistribution());

        assertEquals(3, RemoteVersion.OPENSEARCH_3_1_0.getMajor());
        assertEquals(1, RemoteVersion.OPENSEARCH_3_1_0.getMinor());
        assertEquals(0, RemoteVersion.OPENSEARCH_3_1_0.getRevision());
        assertEquals("opensearch", RemoteVersion.OPENSEARCH_3_1_0.getDistribution());
    }

    public void testVersionOrdering() {
        // Test chronological ordering of constants
        assertTrue(RemoteVersion.V_0_20_5.before(RemoteVersion.V_0_90_13));
        assertTrue(RemoteVersion.V_0_90_13.before(RemoteVersion.V_1_0_0));
        assertTrue(RemoteVersion.V_1_0_0.before(RemoteVersion.V_1_7_5));
        assertTrue(RemoteVersion.V_1_7_5.before(RemoteVersion.V_2_0_0));
        assertTrue(RemoteVersion.V_2_0_0.before(RemoteVersion.V_2_1_0));
        assertTrue(RemoteVersion.V_2_1_0.before(RemoteVersion.V_2_3_3));
        assertTrue(RemoteVersion.V_2_3_3.before(RemoteVersion.V_5_0_0));
        assertTrue(RemoteVersion.V_5_0_0.before(RemoteVersion.V_6_0_0));
        assertTrue(RemoteVersion.V_6_0_0.before(RemoteVersion.V_6_3_0));
        assertTrue(RemoteVersion.V_6_3_0.before(RemoteVersion.V_7_0_0));
    }

    public void testDistributionChecks() {
        // Test isElasticsearch()
        assertTrue(RemoteVersion.V_7_0_0.isElasticsearch());
        assertFalse(RemoteVersion.OPENSEARCH_1_0_0.isElasticsearch());

        // Test isOpenSearch()
        assertTrue(RemoteVersion.OPENSEARCH_1_0_0.isOpenSearch());
        assertFalse(RemoteVersion.V_7_0_0.isOpenSearch());

        // Test custom distribution
        RemoteVersion customVersion = new RemoteVersion(1, 0, 0, "custom");
        assertFalse(customVersion.isElasticsearch());
        assertFalse(customVersion.isOpenSearch());
        assertEquals("custom", customVersion.getDistribution());

        // Test null distribution defaults to elasticsearch
        RemoteVersion nullDistVersion = new RemoteVersion(1, 0, 0, null);
        assertTrue(nullDistVersion.isElasticsearch());
        assertFalse(nullDistVersion.isOpenSearch());
    }

    public void testEqualsAndHashCode() {
        RemoteVersion version1 = new RemoteVersion(2, 0, 0, null);
        RemoteVersion version2 = new RemoteVersion(2, 0, 0, null);
        RemoteVersion version3 = new RemoteVersion(2, 0, 0, "opensearch");
        RemoteVersion version4 = new RemoteVersion(2, 0, 1, null);

        // Test equals
        assertEquals(version1, version2);
        assertNotEquals(version1, version3); // Different distribution
        assertNotEquals(version1, version4); // Different revision
        assertNotEquals(version1, null);
        assertNotEquals(version1, "not a version");

        // Test hashCode consistency
        assertEquals(version1.hashCode(), version2.hashCode());
        assertNotEquals(version1.hashCode(), version3.hashCode());

        // Test reflexivity
        assertEquals(version1, version1);
    }

    public void testToString() {
        RemoteVersion version = new RemoteVersion(2, 1, 3, null);
        assertEquals("2.1.3", version.toString());

        RemoteVersion opensearchVersion = new RemoteVersion(1, 0, 0, "opensearch");
        assertEquals("1.0.0", opensearchVersion.toString());
    }

    public void testCompareToWithNull() {
        RemoteVersion version = RemoteVersion.V_2_0_0;
        assertTrue(version.compareTo(null) > 0);
    }

    public void testVersionConstantsAreCorrect() {
        // Verify that our constants match what fromString would produce
        assertEquals(RemoteVersion.V_2_0_0, RemoteVersion.fromString("2.0.0"));
        assertEquals(RemoteVersion.V_7_0_0, RemoteVersion.fromString("7.0.0"));
        assertEquals(RemoteVersion.OPENSEARCH_1_0_0, RemoteVersion.fromString("1.0.0", "opensearch"));
        assertEquals(RemoteVersion.OPENSEARCH_2_0_0, RemoteVersion.fromString("2.0.0", "opensearch"));
    }

    public void testCaseInsensitiveDistribution() {
        RemoteVersion version1 = new RemoteVersion(1, 0, 0, null);
        RemoteVersion version2 = new RemoteVersion(1, 0, 0, "elasticsearch");
        RemoteVersion version3 = new RemoteVersion(1, 0, 0, "ElasticSearch");

        assertEquals(version1, version2);
        assertEquals(version1, version3);
        assertTrue(version1.isElasticsearch());
        assertTrue(version2.isElasticsearch());
        assertTrue(version3.isElasticsearch());
    }
}
