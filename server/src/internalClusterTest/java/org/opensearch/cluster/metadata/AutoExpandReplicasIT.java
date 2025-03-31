/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AutoExpandReplicasIT extends OpenSearchIntegTestCase {

    public void testAutoExpandReplica() throws Exception {
        String indexName = "test";
        internalCluster().startClusterManagerOnlyNode();
        // Create a cluster with 2 data nodes
        String primaryNode = internalCluster().startDataOnlyNode();
        // Create index with 1 Primary and 1 Replica shard
        createIndex(
            indexName,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(0))
                .build()
        );
        ensureYellow();
        internalCluster().startDataOnlyNode();
        ensureGreen();

        NumShards numShards = getNumShards(indexName);
        assertEquals(2, numShards.totalNumShards);

        // Enable Auto expand on the index
        client().admin().indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.auto_expand_replicas", "0-all"))
            .get();

        // Add 2 more data nodes
        internalCluster().startDataOnlyNodes(2);
        assertBusy(() -> assertEquals(4, getNumShards(indexName).totalNumShards));

        // Stop a node which hosts primary shard
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
        assertBusy(() -> assertEquals(3, getNumShards(indexName).totalNumShards));

        // Add 2 more data nodes
        internalCluster().startDataOnlyNodes(2);
        assertBusy(() -> assertEquals(5, getNumShards(indexName).totalNumShards));
    }
}
