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

        // Test OpenSearch vs Elasticsearch versions with same numbers
        assertEquals(0, RemoteVersion.ELASTICSEARCH_2_0_0.compareTo(RemoteVersion.OPENSEARCH_2_0_0)); // Same version numbers should be equal in
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
        assertEquals(0, RemoteVersion.ELASTICSEARCH_0_20_5.getMajor());
        assertEquals(20, RemoteVersion.ELASTICSEARCH_0_20_5.getMinor());
        assertEquals(5, RemoteVersion.ELASTICSEARCH_0_20_5.getRevision());
        assertEquals("elasticsearch", RemoteVersion.ELASTICSEARCH_0_20_5.getDistribution());

        assertEquals(7, RemoteVersion.ELASTICSEARCH_7_0_0.getMajor());
        assertEquals(0, RemoteVersion.ELASTICSEARCH_7_0_0.getMinor());
        assertEquals(0, RemoteVersion.ELASTICSEARCH_7_0_0.getRevision());
        assertEquals("elasticsearch", RemoteVersion.ELASTICSEARCH_7_0_0.getDistribution());

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

    public void testDistributionChecks() {
        // Test isElasticsearch()
        assertTrue(RemoteVersion.ELASTICSEARCH_7_0_0.isElasticsearch());
        assertFalse(RemoteVersion.OPENSEARCH_1_0_0.isElasticsearch());

        // Test isOpenSearch()
        assertTrue(RemoteVersion.OPENSEARCH_1_0_0.isOpenSearch());
        assertFalse(RemoteVersion.ELASTICSEARCH_7_0_0.isOpenSearch());

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

    /**
     * Test equals method
     */
    public void testEqualsMethodAllBranches() {
        RemoteVersion baseVersion = new RemoteVersion(2, 1, 3, "elasticsearch");

        // Test same object (reflexivity)
        assertEquals(baseVersion, baseVersion);

        // Test null
        assertNotEquals(baseVersion, null);

        // Test different class
        assertNotEquals(baseVersion, "not a version object");

        // Test different major version
        RemoteVersion differentMajor = new RemoteVersion(1, 1, 3, "elasticsearch");
        assertNotEquals(baseVersion, differentMajor);

        // Test different minor version
        RemoteVersion differentMinor = new RemoteVersion(2, 0, 3, "elasticsearch");
        assertNotEquals(baseVersion, differentMinor);

        // Test different revision
        RemoteVersion differentRevision = new RemoteVersion(2, 1, 2, "elasticsearch");
        assertNotEquals(baseVersion, differentRevision);

        // Test different distribution (null vs non-null) - null becomes "elasticsearch", so they are equal
        RemoteVersion nullDistribution = new RemoteVersion(2, 1, 3, null);
        assertEquals(baseVersion, nullDistribution); // null distribution becomes "elasticsearch"

        // Test different distribution (non-null vs non-null)
        RemoteVersion opensearchDistribution = new RemoteVersion(2, 1, 3, "opensearch");
        assertNotEquals(baseVersion, opensearchDistribution);

        // Test same distribution (both null)
        RemoteVersion nullDist1 = new RemoteVersion(2, 1, 3, null);
        RemoteVersion nullDist2 = new RemoteVersion(2, 1, 3, null);
        assertEquals(nullDist1, nullDist2);

        // Test identical version (all fields match)
        RemoteVersion identical = new RemoteVersion(2, 1, 3, "elasticsearch");
        assertEquals(baseVersion, identical);

        // Test case sensitivity in distribution - both become lowercase, so they are equal
        RemoteVersion upperCaseDist = new RemoteVersion(2, 1, 3, "ELASTICSEARCH");
        assertEquals(baseVersion, upperCaseDist); // Both become "elasticsearch" due to toLowerCase()
    }

    public void testToString() {
        RemoteVersion version = new RemoteVersion(2, 1, 3, null);
        assertEquals("2.1.3", version.toString());

        RemoteVersion opensearchVersion = new RemoteVersion(1, 0, 0, "opensearch");
        assertEquals("1.0.0", opensearchVersion.toString());
    }

    public void testCompareToWithNull() {
        RemoteVersion version = RemoteVersion.ELASTICSEARCH_2_0_0;
        assertTrue(version.compareTo(null) > 0);
    }

    public void testVersionConstantsAreCorrect() {
        // Verify that our constants match what fromString would produce
        assertEquals(RemoteVersion.ELASTICSEARCH_2_0_0, RemoteVersion.fromString("2.0.0"));
        assertEquals(RemoteVersion.ELASTICSEARCH_7_0_0, RemoteVersion.fromString("7.0.0"));
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
