/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class BaseRemoteStoreRestoreIT extends RemoteStoreBaseIntegTestCase {
    static final String INDEX_NAME = "remote-store-test-idx-1";
    static final String INDEX_NAMES = "test-remote-store-1,test-remote-store-2,remote-store-test-index-1,remote-store-test-index-2";
    static final String INDEX_NAMES_WILDCARD = "test-remote-store-*,remote-store-test-index-*";
    static final String TOTAL_OPERATIONS = "total-operations";
    static final String REFRESHED_OR_FLUSHED_OPERATIONS = "refreshed-or-flushed-operations";
    static final String MAX_SEQ_NO_TOTAL = "max-seq-no-total";

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings(0);
    }

    public Settings indexSettings(int shards, int replicas) {
        return remoteStoreIndexSettings(replicas, shards);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class)).collect(Collectors.toList());
    }

    protected void restore(String... indices) {
        restore(randomBoolean(), indices);
    }

    protected void verifyRestoredData(Map<String, Long> indexStats, String indexName, boolean indexMoreData) throws Exception {
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        // This is to ensure that shards that were already assigned will get latest count
        refresh(indexName);
        assertBusy(
            () -> assertHitCount(client().prepareSearch(indexName).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS)),
            30,
            TimeUnit.SECONDS
        );
        if (indexMoreData == false) return;

        IndexResponse response = indexSingleDoc(indexName);
        if (indexStats.containsKey(MAX_SEQ_NO_TOTAL + "-shard-" + response.getShardId().id())) {
            assertEquals(indexStats.get(MAX_SEQ_NO_TOTAL + "-shard-" + response.getShardId().id()) + 1, response.getSeqNo());
        }
        refresh(indexName);
        assertBusy(
            () -> assertHitCount(client().prepareSearch(indexName).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS) + 1),
            30,
            TimeUnit.SECONDS
        );
    }

    protected void verifyRestoredData(Map<String, Long> indexStats, String indexName) throws Exception {
        verifyRestoredData(indexStats, indexName, true);
    }

    public void prepareCluster(int numClusterManagerNodes, int numDataOnlyNodes, String indices, int replicaCount, int shardCount) {
        prepareCluster(numClusterManagerNodes, numDataOnlyNodes, indices, replicaCount, shardCount, Settings.EMPTY);
    }

    public void prepareCluster(
        int numClusterManagerNodes,
        int numDataOnlyNodes,
        String indices,
        int replicaCount,
        int shardCount,
        Settings settings
    ) {
        prepareCluster(numClusterManagerNodes, numDataOnlyNodes, settings);
        for (String index : indices.split(",")) {
            createIndex(index, remoteStoreIndexSettings(replicaCount, shardCount));
            ensureYellowAndNoInitializingShards(index);
            ensureGreen(index);
        }
    }

    public void prepareCluster(int numClusterManagerNodes, int numDataOnlyNodes, Settings settings) {
        internalCluster().startClusterManagerOnlyNodes(numClusterManagerNodes, settings);
        internalCluster().startDataOnlyNodes(numDataOnlyNodes, settings);
    }
}
