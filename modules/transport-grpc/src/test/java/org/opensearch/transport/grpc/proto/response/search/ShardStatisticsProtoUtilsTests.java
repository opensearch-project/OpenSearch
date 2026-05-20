/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.protobufs.ShardStatistics;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class ShardStatisticsProtoUtilsTests extends OpenSearchTestCase {

    public void testGetShardStatsBasicFields() throws IOException {
        ShardStatistics stats = ShardStatisticsProtoUtils.getShardStats(10, 8, 1, 1, new ShardOperationFailedException[0]);

        assertEquals("Total shards should match", 10, stats.getTotal());
        assertEquals("Successful shards should match", 8, stats.getSuccessful());
        assertEquals("Skipped shards should match", 1, stats.getSkipped());
        assertEquals("Failed shards should match", 1, stats.getFailed());
    }

    public void testGetShardStatsWithNoSkipped() throws IOException {
        // When skipped is negative, it should not be set
        ShardStatistics stats = ShardStatisticsProtoUtils.getShardStats(10, 9, -1, 1, new ShardOperationFailedException[0]);

        assertEquals("Total shards should match", 10, stats.getTotal());
        assertEquals("Successful shards should match", 9, stats.getSuccessful());
        assertFalse("Skipped should not be set when negative", stats.hasSkipped());
        assertEquals("Failed shards should match", 1, stats.getFailed());
    }

    public void testGetShardStatsWithNoFailures() throws IOException {
        ShardStatistics stats = ShardStatisticsProtoUtils.getShardStats(10, 10, 0, 0, new ShardOperationFailedException[0]);

        assertEquals("Total shards should match", 10, stats.getTotal());
        assertEquals("Successful shards should match", 10, stats.getSuccessful());
        assertEquals("Failed shards should be 0", 0, stats.getFailed());
        assertEquals("Failures list should be empty", 0, stats.getFailuresCount());
        assertEquals("Failures2 list should be empty", 0, stats.getFailures2Count());
    }

    public void testGetShardStatsWithNullFailures() throws IOException {
        ShardStatistics stats = ShardStatisticsProtoUtils.getShardStats(10, 9, 0, 1, null);

        assertEquals("Total shards should match", 10, stats.getTotal());
        assertEquals("Successful shards should match", 9, stats.getSuccessful());
        assertEquals("Failed shards should match", 1, stats.getFailed());
        assertEquals("Failures list should be empty when null", 0, stats.getFailuresCount());
        assertEquals("Failures2 list should be empty when null", 0, stats.getFailures2Count());
    }

    public void testGetShardStatsWithShardSearchFailures() throws IOException {
        // Create ShardSearchFailure instances
        ShardId shardId1 = new ShardId("test_index", "_na_", 1);
        SearchShardTarget target1 = new SearchShardTarget("node1", shardId1, null, null);
        ShardSearchFailure failure1 = new ShardSearchFailure(new Exception("failure 1"), target1);

        ShardId shardId2 = new ShardId("test_index", "_na_", 2);
        SearchShardTarget target2 = new SearchShardTarget("node2", shardId2, null, null);
        ShardSearchFailure failure2 = new ShardSearchFailure(new Exception("failure 2"), target2);

        ShardOperationFailedException[] failures = new ShardOperationFailedException[] { failure1, failure2 };

        // Get stats
        ShardStatistics stats = ShardStatisticsProtoUtils.getShardStats(10, 8, 0, 2, failures);

        assertEquals("Total shards should match", 10, stats.getTotal());
        assertEquals("Successful shards should match", 8, stats.getSuccessful());
        assertEquals("Failed shards should match", 2, stats.getFailed());

        // Verify both the legacy failures field and new failures_2 field are populated
        assertTrue("Legacy failures field should have entries", stats.getFailuresCount() > 0);
        assertTrue("New failures_2 field should have entries", stats.getFailures2Count() > 0);

        // For ShardSearchFailure, both should have the same count
        assertEquals(
            "Both failures fields should have same count for search failures",
            stats.getFailuresCount(),
            stats.getFailures2Count()
        );
    }

    public void testGetShardStatsPopulatesBothFieldsForBackwardCompatibility() throws IOException {
        // Create a ShardSearchFailure
        ShardId shardId = new ShardId("compat_index", "_na_", 3);
        SearchShardTarget target = new SearchShardTarget("compat_node", shardId, null, null);
        ShardSearchFailure failure = new ShardSearchFailure(new Exception("compat failure"), target);

        ShardOperationFailedException[] failures = new ShardOperationFailedException[] { failure };

        // Get stats
        ShardStatistics stats = ShardStatisticsProtoUtils.getShardStats(5, 4, 0, 1, failures);

        // Verify both fields are populated
        assertEquals("Legacy failures field should have 1 entry", 1, stats.getFailuresCount());
        assertEquals("New failures_2 field should have 1 entry", 1, stats.getFailures2Count());

        // Verify the legacy field contains a ShardFailure proto
        assertTrue("Legacy failures should have at least one entry", stats.getFailuresList().size() > 0);
        org.opensearch.protobufs.ShardFailure legacyFailure = stats.getFailures(0);
        assertNotNull("Legacy failure should not be null", legacyFailure);
        assertEquals("Legacy failure index should match", "compat_index", legacyFailure.getIndex());
        assertEquals("Legacy failure shard should match", 3, legacyFailure.getShard());

        // Verify the new field contains a ShardSearchFailure proto
        assertTrue("New failures_2 should have at least one entry", stats.getFailures2List().size() > 0);
        org.opensearch.protobufs.ShardSearchFailure newFailure = stats.getFailures2(0);
        assertNotNull("New failure should not be null", newFailure);
        assertEquals("New failure index should match", "compat_index", newFailure.getIndex());
        assertEquals("New failure shard should match", 3, newFailure.getShard());

        // Verify common fields match between old and new
        assertEquals("Index should match between old and new", legacyFailure.getIndex(), newFailure.getIndex());
        assertEquals("Shard should match between old and new", legacyFailure.getShard(), newFailure.getShard());
        assertEquals("Node should match between old and new", legacyFailure.getNode(), newFailure.getNode());
    }

    public void testGetShardStatsWithMixedFailureTypes() throws IOException {
        // Create a ShardSearchFailure (will populate both fields)
        ShardId shardId1 = new ShardId("mixed_index", "_na_", 1);
        SearchShardTarget target1 = new SearchShardTarget("node1", shardId1, null, null);
        ShardSearchFailure searchFailure = new ShardSearchFailure(new Exception("search failure"), target1);

        // For this test, we're only testing ShardSearchFailure since that's what populates failures_2
        // Other failure types would only populate the legacy failures field
        ShardOperationFailedException[] failures = new ShardOperationFailedException[] { searchFailure };

        // Get stats
        ShardStatistics stats = ShardStatisticsProtoUtils.getShardStats(5, 4, 0, 1, failures);

        // The legacy field should have all failures
        assertEquals("Legacy failures field should have 1 entry", 1, stats.getFailuresCount());

        // The new failures_2 field should only have ShardSearchFailure instances
        assertEquals("New failures_2 field should have 1 entry (only ShardSearchFailure)", 1, stats.getFailures2Count());
    }

    /**
     * Integration test: Verifies that both old and new fields are populated
     * with multiple shard search failures for backward compatibility.
     */
    public void testDualFieldPopulationWithMultipleFailures() throws IOException {
        // Setup: Create multiple shard search failures
        ShardId shard1 = new ShardId("index1", "_na_", 0);
        ShardId shard2 = new ShardId("index2", "_na_", 1);
        ShardId shard3 = new ShardId("index3", "_na_", 2);

        SearchShardTarget target1 = new SearchShardTarget("node1", shard1, null, null);
        SearchShardTarget target2 = new SearchShardTarget("node2", shard2, null, null);
        SearchShardTarget target3 = new SearchShardTarget("node3", shard3, null, null);

        ShardSearchFailure failure1 = new ShardSearchFailure(new RuntimeException("Timeout on shard 0"), target1);
        ShardSearchFailure failure2 = new ShardSearchFailure(new RuntimeException("OOM on shard 1"), target2);
        ShardSearchFailure failure3 = new ShardSearchFailure(new RuntimeException("Circuit breaker on shard 2"), target3);

        ShardOperationFailedException[] failures = new ShardOperationFailedException[] { failure1, failure2, failure3 };

        // Execute: Convert to proto
        ShardStatistics stats = ShardStatisticsProtoUtils.getShardStats(10, 7, 0, 3, failures);

        // Verify: Both fields should be populated
        assertEquals("Should have 3 failures in legacy field", 3, stats.getFailuresCount());
        assertEquals("Should have 3 failures in new field", 3, stats.getFailures2Count());

        // Verify: Data consistency between old and new fields
        for (int i = 0; i < stats.getFailuresCount(); i++) {
            org.opensearch.protobufs.ShardFailure legacyFailure = stats.getFailures(i);
            org.opensearch.protobufs.ShardSearchFailure newFailure = stats.getFailures2(i);

            assertNotNull("Legacy failure " + i + " should not be null", legacyFailure);
            assertNotNull("New failure " + i + " should not be null", newFailure);

            assertEquals("Index should match between legacy and new at position " + i, legacyFailure.getIndex(), newFailure.getIndex());
            assertEquals("Shard should match between legacy and new at position " + i, legacyFailure.getShard(), newFailure.getShard());
            assertEquals("Node should match between legacy and new at position " + i, legacyFailure.getNode(), newFailure.getNode());
        }
    }

    /**
     * Integration test: Simulates old client reading from deprecated 'failures' field.
     */
    public void testOldClientCanReadLegacyField() throws IOException {
        ShardId shardId = new ShardId("old_client_test", "_na_", 5);
        SearchShardTarget target = new SearchShardTarget("old_node", shardId, null, null);
        ShardSearchFailure failure = new ShardSearchFailure(new Exception("old client error"), target);

        ShardOperationFailedException[] failures = new ShardOperationFailedException[] { failure };

        ShardStatistics stats = ShardStatisticsProtoUtils.getShardStats(1, 0, 0, 1, failures);

        // Old client reads from 'failures' field
        assertTrue("Old client should see failures in legacy field", stats.getFailuresCount() > 0);
        org.opensearch.protobufs.ShardFailure legacyFailure = stats.getFailures(0);

        // Old client can extract all necessary information
        assertEquals("Old client can read index", "old_client_test", legacyFailure.getIndex());
        assertEquals("Old client can read shard", 5, legacyFailure.getShard());
        assertEquals("Old client can read node", "old_node", legacyFailure.getNode());
        assertTrue("Old client can read reason", legacyFailure.hasReason());

        // Note: Old client will see primary=false (default), which is acceptable for search failures
        assertFalse("Primary defaults to false for search failures", legacyFailure.getPrimary());
    }

    /**
     * Integration test: Simulates new client reading from 'failures_2' field
     * which uses ShardSearchFailure proto type (without primary field).
     */
    public void testNewClientCanReadNewField() throws IOException {
        ShardId shardId = new ShardId("new_client_test", "_na_", 7);
        SearchShardTarget target = new SearchShardTarget("new_node", shardId, null, null);
        ShardSearchFailure failure = new ShardSearchFailure(new Exception("new client error"), target);

        ShardOperationFailedException[] failures = new ShardOperationFailedException[] { failure };

        ShardStatistics stats = ShardStatisticsProtoUtils.getShardStats(1, 0, 0, 1, failures);

        // New client reads from 'failures_2' field
        assertTrue("New client should see failures in new field", stats.getFailures2Count() > 0);
        org.opensearch.protobufs.ShardSearchFailure newFailure = stats.getFailures2(0);

        // New client can extract all necessary information
        assertEquals("New client can read index", "new_client_test", newFailure.getIndex());
        assertEquals("New client can read shard", 7, newFailure.getShard());
        assertEquals("New client can read node", "new_node", newFailure.getNode());
        assertTrue("New client can read reason", newFailure.hasReason());

        // New client benefits from not having the 'primary' field in the proto definition
        // so there's no confusion about whether it's required or what it means for search failures
    }

    /**
     * Integration test: Demonstrates gradual migration support where both
     * old and new clients can work simultaneously during the transition period.
     */
    public void testGradualMigrationSupport() throws IOException {
        // During migration: Some clients use old proto, some use new proto
        ShardId shardId = new ShardId("migration_test", "_na_", 9);
        SearchShardTarget target = new SearchShardTarget("migration_node", shardId, null, null);
        ShardSearchFailure failure = new ShardSearchFailure(new Exception("migration error"), target);

        ShardOperationFailedException[] failures = new ShardOperationFailedException[] { failure };

        ShardStatistics stats = ShardStatisticsProtoUtils.getShardStats(2, 1, 0, 1, failures);

        // Both fields are populated
        assertTrue("Legacy field should be populated during migration", stats.getFailuresCount() > 0);
        assertTrue("New field should be populated during migration", stats.getFailures2Count() > 0);

        // Old clients continue working (read from failures)
        org.opensearch.protobufs.ShardFailure legacyFailure = stats.getFailures(0);
        assertNotNull("Old clients can still deserialize", legacyFailure);

        // New clients can start using new field (read from failures_2)
        org.opensearch.protobufs.ShardSearchFailure newFailure = stats.getFailures2(0);
        assertNotNull("New clients can deserialize new field", newFailure);

        // Both get correct data
        assertEquals("Both clients see same index", legacyFailure.getIndex(), newFailure.getIndex());
        assertEquals("Both clients see same shard", legacyFailure.getShard(), newFailure.getShard());
    }
}
