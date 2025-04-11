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
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AutoExpandSearchReplicasIT extends RemoteStoreBaseIntegTestCase {

    public void testAutoExpandSearchReplica() throws Exception {
        String indexName = "test";
        internalCluster().startClusterManagerOnlyNode();

        // Create a cluster with 2 data nodes and 1 search node
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        String searchNode = internalCluster().startSearchOnlyNode();

        // Create index with 1 primary, 1 replica and 1 search replica shards
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(0))
                .build()
        );
        ensureGreen();

        assertBusy(() -> assertEquals(1, getNumShards(indexName).numSearchReplicas));

        // Enable auto expand for search replica
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put("index.auto_expand_search_replicas", "0-all"))
            .get();

        // Add 1 more search nodes
        internalCluster().startSearchOnlyNode();

        assertBusy(() -> assertEquals(2, getNumShards(indexName).numSearchReplicas));

        // Stop a node which hosts search replica
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(searchNode));
        assertBusy(() -> assertEquals(1, getNumShards(indexName).numSearchReplicas));

        // Add 1 more search nodes
        internalCluster().startSearchOnlyNode();
        assertBusy(() -> assertEquals(2, getNumShards(indexName).numSearchReplicas));
    }
}
