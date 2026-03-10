/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.exceptions.shardoperationfailedexception;

import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.protobufs.ShardFailure;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class ShardSearchFailureProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithNodeId() throws IOException {
        // Create a SearchShardTarget with a nodeId
        ShardId shardId = new ShardId("test_index", "_na_", 1);
        SearchShardTarget searchShardTarget = new SearchShardTarget("test_node", shardId, null, null);

        // Create a ShardSearchFailure
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(new Exception("fake exception"), searchShardTarget);

        // Call the method under test
        org.opensearch.protobufs.ShardSearchFailure protoFailure = ShardSearchFailureProtoUtils.toProto(shardSearchFailure);

        // Verify the result
        assertNotNull("Proto failure should not be null", protoFailure);
        assertEquals("Index should match", "test_index", protoFailure.getIndex());
        assertEquals("Shard ID should match", 1, protoFailure.getShard());
        assertEquals("Node ID should match", "test_node", protoFailure.getNode());
        assertTrue("Reason should be set", protoFailure.hasReason());

        // Verify primary field is NOT present (ShardSearchFailure proto doesn't have it)
        // This is verified by the proto type itself - ShardSearchFailure doesn't define primary field
    }

    public void testToProtoWithoutNodeId() throws IOException {
        // Create a ShardSearchFailure without nodeId (shard target is null)
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(new Exception("fake exception"));

        // Call the method under test
        org.opensearch.protobufs.ShardSearchFailure protoFailure = ShardSearchFailureProtoUtils.toProto(shardSearchFailure);

        // Verify the result
        assertNotNull("Proto failure should not be null", protoFailure);
        assertTrue("Reason should be set", protoFailure.hasReason());
        // Node should not be set when shard target is null
        assertFalse("Node should not be set", protoFailure.hasNode());
    }

    public void testToLegacyProtoWithNodeId() throws IOException {
        // Create a SearchShardTarget with a nodeId
        ShardId shardId = new ShardId("legacy_index", "_na_", 2);
        SearchShardTarget searchShardTarget = new SearchShardTarget("legacy_node", shardId, null, null);

        // Create a ShardSearchFailure
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(new Exception("legacy exception"), searchShardTarget);

        // Call the legacy method
        ShardFailure protoFailure = ShardSearchFailureProtoUtils.toLegacyProto(shardSearchFailure);

        // Verify the result
        assertNotNull("Proto failure should not be null", protoFailure);
        assertEquals("Index should match", "legacy_index", protoFailure.getIndex());
        assertEquals("Shard ID should match", 2, protoFailure.getShard());
        assertEquals("Node ID should match", "legacy_node", protoFailure.getNode());
        assertTrue("Reason should be set", protoFailure.hasReason());

        // Note: primary field exists in ShardFailure proto but is not set for search failures
        // It will default to false
        assertFalse("Primary should be false (default) for search failures", protoFailure.getPrimary());
    }

    public void testToLegacyProtoWithoutNodeId() throws IOException {
        // Create a ShardSearchFailure without nodeId
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(new Exception("legacy exception without node"));

        // Call the legacy method
        ShardFailure protoFailure = ShardSearchFailureProtoUtils.toLegacyProto(shardSearchFailure);

        // Verify the result
        assertNotNull("Proto failure should not be null", protoFailure);
        assertTrue("Reason should be set", protoFailure.hasReason());
        // Node should not be set when shard target is null
        assertFalse("Node should not be set", protoFailure.hasNode());
    }

    public void testToProtoVsLegacyProtoHaveSameData() throws IOException {
        // Create test data
        ShardId shardId = new ShardId("compare_index", "_na_", 3);
        SearchShardTarget searchShardTarget = new SearchShardTarget("compare_node", shardId, null, null);
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure(new Exception("compare exception"), searchShardTarget);

        // Get both proto types
        org.opensearch.protobufs.ShardSearchFailure newProto = ShardSearchFailureProtoUtils.toProto(shardSearchFailure);
        ShardFailure legacyProto = ShardSearchFailureProtoUtils.toLegacyProto(shardSearchFailure);

        // Verify common fields have same values
        assertEquals("Index should match between new and legacy", legacyProto.getIndex(), newProto.getIndex());
        assertEquals("Shard should match between new and legacy", legacyProto.getShard(), newProto.getShard());
        assertEquals("Node should match between new and legacy", legacyProto.getNode(), newProto.getNode());

        // Both should have reason set
        assertTrue("New proto should have reason", newProto.hasReason());
        assertTrue("Legacy proto should have reason", legacyProto.hasReason());
    }
}
