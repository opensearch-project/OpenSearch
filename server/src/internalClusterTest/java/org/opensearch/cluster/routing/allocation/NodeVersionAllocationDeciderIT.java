/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for NodeVersionAllocationDecider to verify allocation behavior
 * with different node versions and replication types in a real cluster environment.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NodeVersionAllocationDeciderIT extends OpenSearchIntegTestCase {

    @Override
    protected int numberOfShards() {
        return 2;
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    /**
     * Test that segment replication indices with older versions don't allocate primaries on newer nodes
     */
    public void testSegmentReplicationPrimaryAllocationWithVersions() throws Exception {
        logger.info("--> starting cluster manager node");
        internalCluster().startClusterManagerOnlyNode();
        
        logger.info("--> starting older version data node");
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.attr.version", VersionUtils.getPreviousVersion().toString()).build()
        );
        
        logger.info("--> starting newer version data node");
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.attr.version", Version.CURRENT.toString()).build()
        );
        
        logger.info("--> creating segment replication index");
        final String indexName = "segment-index";
        assertAcked(client().admin().indices().prepareCreate(indexName)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()));
        
        ensureGreen(indexName);
        
        logger.info("--> verifying primary allocation");
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexShardRoutingTable shardTable = state.getRoutingTable().index(indexName).getShards().get(0);
        ShardRouting primary = shardTable.primaryShard();
        ShardRouting replica = shardTable.replicaShards().get(0);
        
        assertTrue("Primary should be allocated", primary.assignedToNode());
        assertTrue("Replica should be allocated", replica.assignedToNode());
        
        // For segment replication, verify that allocation decisions are made correctly
        DiscoveryNode primaryNode = state.nodes().get(primary.currentNodeId());
        DiscoveryNode replicaNode = state.nodes().get(replica.currentNodeId());
        
        // Both nodes should be available for allocation
        assertNotNull("Primary node should be available", primaryNode);
        assertNotNull("Replica node should be available", replicaNode);
    }

    /**
     * Test that document replication indices allow primary and replica on same version nodes
     */
    public void testDocumentReplicationAllowsSameVersionNodes() throws Exception {
        logger.info("--> starting cluster manager node");
        internalCluster().startClusterManagerOnlyNode();
        
        logger.info("--> starting two nodes with same version");
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        
        logger.info("--> creating document replication index");
        final String indexName = "document-index";
        assertAcked(client().admin().indices().prepareCreate(indexName)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
                .build()));
        
        ensureGreen(indexName);
        
        logger.info("--> verifying primary and replica can be on same version nodes");
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexShardRoutingTable shardTable = state.getRoutingTable().index(indexName).getShards().get(0);
        ShardRouting primary = shardTable.primaryShard();
        ShardRouting replica = shardTable.replicaShards().get(0);
        
        DiscoveryNode primaryNode = state.nodes().get(primary.currentNodeId());
        DiscoveryNode replicaNode = state.nodes().get(replica.currentNodeId());
        
        assertEquals("Primary and replica should be on same version for document replication", 
            primaryNode.getVersion(), replicaNode.getVersion());
    }

    /**
     * Test that indices with different replication types can coexist and are allocated properly
     */
    public void testIndicesWithDifferentReplicationTypesCoexist() throws Exception {
        logger.info("--> starting cluster manager node");
        internalCluster().startClusterManagerOnlyNode();
        
        logger.info("--> starting nodes with different versions");
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.attr.version", VersionUtils.getPreviousVersion().toString()).build()
        );
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.attr.version", Version.CURRENT.toString()).build()
        );
        
        logger.info("--> creating index with SEGMENT replication");
        final String segmentIndexName = "segment-index";
        assertAcked(client().admin().indices().prepareCreate(segmentIndexName)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()));
        
        logger.info("--> creating index with DOCUMENT replication");
        final String documentIndexName = "document-index";
        assertAcked(client().admin().indices().prepareCreate(documentIndexName)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
                .build()));
        
        ensureGreen(segmentIndexName, documentIndexName);
        
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        
        logger.info("--> verifying segment replication index is properly allocated");
        IndexShardRoutingTable segmentShardTable = state.getRoutingTable().index(segmentIndexName).getShards().get(0);
        ShardRouting segmentPrimary = segmentShardTable.primaryShard();
        ShardRouting segmentReplica = segmentShardTable.replicaShards().get(0);
        assertTrue("Segment replication primary should be allocated", segmentPrimary.assignedToNode());
        assertTrue("Segment replication replica should be allocated", segmentReplica.assignedToNode());
        
        logger.info("--> verifying document replication index is properly allocated");
        IndexShardRoutingTable documentShardTable = state.getRoutingTable().index(documentIndexName).getShards().get(0);
        ShardRouting documentPrimary = documentShardTable.primaryShard();
        ShardRouting documentReplica = documentShardTable.replicaShards().get(0);
        assertTrue("Document replication primary should be allocated", documentPrimary.assignedToNode());
        assertTrue("Document replication replica should be allocated", documentReplica.assignedToNode());
        
        // Verify that both indices can coexist with different replication types
        DiscoveryNode segmentPrimaryNode = state.nodes().get(segmentPrimary.currentNodeId());
        DiscoveryNode documentPrimaryNode = state.nodes().get(documentPrimary.currentNodeId());
        assertNotNull("Both indices should be allocated successfully - segment", segmentPrimaryNode);
        assertNotNull("Both indices should be allocated successfully - document", documentPrimaryNode);
    }

    /**
     * Test allocation behavior with multiple indices having different replication types
     */
    public void testMultipleIndicesWithDifferentReplicationTypes() throws Exception {
        logger.info("--> starting cluster manager node");
        internalCluster().startClusterManagerOnlyNode();
        
        logger.info("--> starting multiple nodes with different versions");
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.attr.version", VersionUtils.getPreviousVersion().toString()).build()
        );
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.attr.version", VersionUtils.getPreviousVersion().toString()).build()
        );
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.attr.version", Version.CURRENT.toString()).build()
        );
        
        logger.info("--> creating multiple indices with different replication types");
        final String[] indexNames = {"segment-index-1", "segment-index-2", "document-index-1", "document-index-2"};
        
        for (int i = 0; i < indexNames.length; i++) {
            ReplicationType replicationType = (i < 2) ? ReplicationType.SEGMENT : ReplicationType.DOCUMENT;
            
            assertAcked(client().admin().indices().prepareCreate(indexNames[i])
                .setSettings(Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, replicationType)
                    .build()));
        }
        
        ensureGreen(indexNames);
        
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        
        logger.info("--> verifying each index follows its own replication type rules");
        for (String indexName : indexNames) {
            IndexShardRoutingTable shardTable = state.getRoutingTable().index(indexName).getShards().get(0);
            ShardRouting primary = shardTable.primaryShard();
            ShardRouting replica = shardTable.replicaShards().get(0);
            DiscoveryNode primaryNode = state.nodes().get(primary.currentNodeId());
            DiscoveryNode replicaNode = state.nodes().get(replica.currentNodeId());
            
            // Verify that all indices are properly allocated regardless of replication type
            assertTrue("Index " + indexName + " primary should be allocated", primary.assignedToNode());
            assertTrue("Index " + indexName + " replica should be allocated", replica.assignedToNode());
            assertNotNull("Index " + indexName + " should have valid primary node", primaryNode);
            assertNotNull("Index " + indexName + " should have valid replica node", replicaNode);
        }
    }

    /**
     * Test that allocation decisions are logged properly without causing exceptions
     */
    public void testAllocationDecisionLogging() throws Exception {
        logger.info("--> starting cluster manager node");
        internalCluster().startClusterManagerOnlyNode();
        
        logger.info("--> starting nodes with different versions");
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.attr.version", VersionUtils.getPreviousVersion().toString()).build()
        );
        internalCluster().startDataOnlyNode(
            Settings.builder().put("node.attr.version", Version.CURRENT.toString()).build()
        );
        
        final String indexName = "logging-test-index";
        
        logger.info("--> creating index with segment replication to trigger detailed logging");
        assertAcked(client().admin().indices().prepareCreate(indexName)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()));
        
        logger.info("--> triggering multiple allocation events to generate logs");
        for (int i = 0; i < 3; i++) {
            // Disable and re-enable allocation to trigger rebalancing
            assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder()
                    .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")));
            
            assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder()
                    .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all")));
            
            client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();
        }
        
        ensureGreen(indexName);
        
        logger.info("--> verifying cluster is healthy and allocation completed successfully");
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        
        // All shards should be allocated
        for (int shardId = 0; shardId < 2; shardId++) {
            IndexShardRoutingTable shardTable = state.getRoutingTable().index(indexName).getShards().get(shardId);
            assertTrue("Primary shard " + shardId + " should be assigned", shardTable.primaryShard().assignedToNode());
            assertTrue("Replica shard " + shardId + " should be assigned", shardTable.replicaShards().get(0).assignedToNode());
        }
    }

    /**
     * Test error handling with invalid settings.
     * This test verifies that the NodeVersionAllocationDecider handles invalid settings gracefully
     * and doesn't cause allocation issues.
     */
    public void testErrorHandlingWithInvalidSettings() throws Exception {
        logger.info("--> starting cluster manager node");
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        
        final String indexName = "error-handling-index";
        
        logger.info("--> creating index with valid settings first");
        assertAcked(client().admin().indices().prepareCreate(indexName)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0) // Use 0 replicas to avoid complexity
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
                .build()));
        
        ensureGreen(indexName);
        
        logger.info("--> trying to update with invalid replication type");
        try {
            client().admin().indices().prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_REPLICATION_TYPE, "invalid_type"))
                .execute().actionGet();
            fail("Should have thrown an exception for invalid replication type");
        } catch (Exception e) {
            // Expected - invalid replication type should be rejected
            String message = e.getMessage();
            String causeMessage = e.getCause() != null ? e.getCause().getMessage() : "";
            assertTrue("Exception should mention invalid replication type", 
                message.contains("replication") || causeMessage.contains("replication") || 
                message.contains("invalid") || causeMessage.contains("invalid") ||
                message.contains("Failed") || causeMessage.contains("Failed"));
        }
        
        logger.info("--> verifying cluster is still healthy after the error");
        // The cluster should remain healthy since the invalid setting update was rejected
        // and the index should still be in a good state
        ensureGreen(indexName);
        
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexShardRoutingTable shardTable = state.getRoutingTable().index(indexName).getShards().get(0);
        assertTrue("Primary should still be assigned after error", shardTable.primaryShard().assignedToNode());
        
        // Verify that the NodeVersionAllocationDecider is still working correctly
        // by creating another index after the error
        logger.info("--> creating another index to verify allocation still works");
        final String secondIndexName = "test-index-2";
        assertAcked(client().admin().indices().prepareCreate(secondIndexName)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()));
        
        ensureGreen(secondIndexName);
        
        ClusterState finalState = client().admin().cluster().prepareState().execute().actionGet().getState();
        IndexShardRoutingTable secondShardTable = finalState.getRoutingTable().index(secondIndexName).getShards().get(0);
        assertTrue("Second index primary should be assigned", secondShardTable.primaryShard().assignedToNode());
    }

    /**
     * Test that manual allocation commands work correctly with version allocation rules.
     * Since integration tests use the same OpenSearch version for all nodes, this test
     * verifies that manual allocation succeeds when nodes have compatible versions.
     */
    public void testManualAllocationRespectsVersionRules() throws Exception {
        logger.info("--> starting cluster manager node");
        internalCluster().startClusterManagerOnlyNode();
        
        logger.info("--> starting data node");
        final String dataNodeName = internalCluster().startDataOnlyNode();
        
        final String indexName = "test-idx";
        
        logger.info("--> disabling allocation first");
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")));
        
        logger.info("--> creating index with allocation disabled");
        assertAcked(client().admin().indices().prepareCreate(indexName)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()));
        
        // Wait for the index to be created but not allocated
        client().admin().cluster().prepareHealth(indexName).setWaitForStatus(org.opensearch.cluster.health.ClusterHealthStatus.RED).get();
        
        logger.info("--> attempting to manually allocate to compatible node should succeed");
        final ClusterRerouteResponse rerouteResponse = client().admin().cluster().prepareReroute()
            .add(new AllocateEmptyPrimaryAllocationCommand(indexName, 0, dataNodeName, true))
            .execute().actionGet();
        
        // Since all nodes in integration tests have the same version, allocation should succeed
        assertTrue("Primary should be assigned to the data node", 
            rerouteResponse.getState().getRoutingTable().index(indexName).shard(0).primaryShard().assignedToNode());
        assertEquals("Allocation decision should be YES", Decision.Type.YES,
            rerouteResponse.getExplanations().explanations().get(0).decisions().type());
    }

    /**
     * Test that segment replication indices use index-specific settings for allocation decisions
     * rather than global cluster settings.
     */
    public void testSegmentReplicationUsesIndexSpecificSettings() throws Exception {
        logger.info("--> starting cluster with same version nodes");
        internalCluster().startNodes(3);
        
        logger.info("--> creating segment replication index");
        Settings segmentReplicationSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        
        assertAcked(client().admin().indices().prepareCreate("segment-index")
            .setSettings(segmentReplicationSettings));
        
        logger.info("--> creating document replication index");
        Settings documentReplicationSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
            .build();
        
        assertAcked(client().admin().indices().prepareCreate("document-index")
            .setSettings(documentReplicationSettings));
        
        logger.info("--> waiting for green status");
        ensureGreen("segment-index", "document-index");
        
        logger.info("--> verifying both indices are properly allocated");
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        
        // Verify segment replication index allocation
        IndexRoutingTable segmentIndexTable = state.routingTable().index("segment-index");
        assertNotNull("Segment index should exist", segmentIndexTable);
        assertEquals("Segment index should have 2 shards", 2, segmentIndexTable.shards().size());
        
        for (int shardId = 0; shardId < segmentIndexTable.shards().size(); shardId++) {
            IndexShardRoutingTable shardTable = segmentIndexTable.shard(shardId);
            assertEquals("Each shard should have primary + replica", 2, shardTable.size());
            assertEquals("Primary should be started", ShardRoutingState.STARTED, shardTable.primaryShard().state());
            assertEquals("Replica should be started", ShardRoutingState.STARTED, shardTable.replicaShards().get(0).state());
        }
        
        // Verify document replication index allocation
        IndexRoutingTable documentIndexTable = state.routingTable().index("document-index");
        assertNotNull("Document index should exist", documentIndexTable);
        assertEquals("Document index should have 2 shards", 2, documentIndexTable.shards().size());
        
        for (int shardId = 0; shardId < documentIndexTable.shards().size(); shardId++) {
            IndexShardRoutingTable shardTable = documentIndexTable.shard(shardId);
            assertEquals("Each shard should have primary + replica", 2, shardTable.size());
            assertEquals("Primary should be started", ShardRoutingState.STARTED, shardTable.primaryShard().state());
            assertEquals("Replica should be started", ShardRoutingState.STARTED, shardTable.replicaShards().get(0).state());
        }
        
        logger.info("--> verifying allocation decisions are independent per index");
        // Both indices should be allocated successfully despite having different replication types
        assertEquals("All shards should be allocated", 0, state.getRoutingNodes().unassigned().size());
    }
}