/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing;

import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.AllocationDecision;
import org.opensearch.cluster.routing.allocation.ShardAllocationDecision;
import org.opensearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MockEngineFactoryPlugin;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.RemoveCorruptedShardDataCommandIT;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.client.Requests;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class AllocationIdIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockEngineFactoryPlugin.class, InternalSettingsPlugin.class);
    }

    public void testFailedRecoveryOnAllocateStalePrimaryRequiresAnotherAllocateStalePrimary() throws Exception {
        /*
         * Allocation id is put on start of shard while historyUUID is adjusted after recovery is done.
         *
         * If during execution of AllocateStalePrimary a proper allocation id is stored in allocation id set and recovery is failed
         * shard restart skips the stage where historyUUID is changed.
         *
         * That leads to situation where allocated stale primary and its replica belongs to the same historyUUID and
         * replica will receive operations after local checkpoint while documents before checkpoints could be significant different.
         *
         * Therefore, on AllocateStalePrimary we put some fake allocation id (no real one could be generated like that)
         * and any failure during recovery requires extra AllocateStalePrimary command to be executed.
         */

        // initial set up
        final String indexName = "index42";
        final String clusterManager = internalCluster().startClusterManagerOnlyNode();
        String node1 = internalCluster().startNode();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "checksum")
                .build()
        );
        final int numDocs = indexDocs(indexName, "foo", "bar");
        final IndexSettings indexSettings = getIndexSettings(indexName, node1);
        final Set<String> allocationIds = getAllocationIds(indexName);
        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final Path indexPath = getIndexPath(node1, shardId);
        assertThat(allocationIds, hasSize(1));
        final String historyUUID = historyUUID(node1, indexName);
        String node2 = internalCluster().startNode();
        ensureGreen(indexName);
        internalCluster().assertSameDocIdsOnShards();
        // initial set up is done

        Settings node1DataPathSettings = internalCluster().dataPathSettings(node1);
        Settings node2DataPathSettings = internalCluster().dataPathSettings(node2);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1));

        // index more docs to node2 that marks node1 as stale
        int numExtraDocs = indexDocs(indexName, "foo", "bar2");
        assertHitCount(client(node2).prepareSearch(indexName).setQuery(matchAllQuery()).get(), numDocs + numExtraDocs);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node2));

        // create fake corrupted marker on node1
        putFakeCorruptionMarker(indexSettings, shardId, indexPath);

        // thanks to cluster-manager node1 is out of sync
        node1 = internalCluster().startNode(node1DataPathSettings);

        // there is only _stale_ primary
        checkNoValidShardCopy(indexName, shardId);

        // allocate stale primary
        client(node1).admin().cluster().prepareReroute().add(new AllocateStalePrimaryAllocationCommand(indexName, 0, node1, true)).get();

        // allocation fails due to corruption marker
        assertBusy(() -> {
            final ClusterState state = client().admin().cluster().prepareState().get().getState();
            final ShardRouting shardRouting = state.routingTable().index(indexName).shard(shardId.id()).primaryShard();
            assertThat(shardRouting.state(), equalTo(ShardRoutingState.UNASSIGNED));
            assertThat(shardRouting.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
        });

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1));
        try (Store store = new Store(shardId, indexSettings, new NIOFSDirectory(indexPath), new DummyShardLock(shardId))) {
            store.removeCorruptionMarker();
        }
        node1 = internalCluster().startNode(node1DataPathSettings);

        // index is red: no any shard is allocated (allocation id is a fake id that does not match to anything)
        checkHealthStatus(indexName, ClusterHealthStatus.RED);
        checkNoValidShardCopy(indexName, shardId);

        // no any valid shard is there; have to invoke AllocateStalePrimary again
        client().admin().cluster().prepareReroute().add(new AllocateStalePrimaryAllocationCommand(indexName, 0, node1, true)).get();

        ensureYellow(indexName);

        // bring node2 back
        node2 = internalCluster().startNode(node2DataPathSettings);
        ensureGreen(indexName);

        assertThat(historyUUID(node1, indexName), not(equalTo(historyUUID)));
        assertThat(historyUUID(node1, indexName), equalTo(historyUUID(node2, indexName)));

        internalCluster().assertSameDocIdsOnShards();
    }

    public void checkHealthStatus(String indexName, ClusterHealthStatus healthStatus) {
        final ClusterHealthStatus indexHealthStatus = client().admin()
            .cluster()
            .health(Requests.clusterHealthRequest(indexName))
            .actionGet()
            .getStatus();
        assertThat(indexHealthStatus, is(healthStatus));
    }

    private int indexDocs(String indexName, Object... source) throws InterruptedException {
        // index some docs in several segments
        int numDocs = 0;
        for (int k = 0, attempts = randomIntBetween(5, 10); k < attempts; k++) {
            final int numExtraDocs = between(10, 100);
            IndexRequestBuilder[] builders = new IndexRequestBuilder[numExtraDocs];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = client().prepareIndex(indexName).setSource(source);
            }

            indexRandom(true, false, true, Arrays.asList(builders));
            numDocs += numExtraDocs;
        }

        return numDocs;
    }

    private Path getIndexPath(String nodeName, ShardId shardId) {
        return RemoveCorruptedShardDataCommandIT.getPathToShardData(nodeName, shardId, ShardPath.INDEX_FOLDER_NAME);
    }

    private Set<String> getAllocationIds(String indexName) {
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final Set<String> allocationIds = state.metadata().index(indexName).inSyncAllocationIds(0);
        return allocationIds;
    }

    private IndexSettings getIndexSettings(String indexName, String nodeName) {
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        final IndexService indexService = indicesService.indexService(resolveIndex(indexName));
        return indexService.getIndexSettings();
    }

    private String historyUUID(String node, String indexName) {
        final ShardStats[] shards = client(node).admin().indices().prepareStats(indexName).clear().get().getShards();
        final String nodeId = client(node).admin().cluster().prepareState().get().getState().nodes().resolveNode(node).getId();
        assertThat(shards.length, greaterThan(0));
        final Set<String> historyUUIDs = Arrays.stream(shards)
            .filter(shard -> shard.getShardRouting().currentNodeId().equals(nodeId))
            .map(shard -> shard.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY))
            .collect(Collectors.toSet());
        assertThat(historyUUIDs, hasSize(1));
        return historyUUIDs.iterator().next();
    }

    private void putFakeCorruptionMarker(IndexSettings indexSettings, ShardId shardId, Path indexPath) throws IOException {
        try (Store store = new Store(shardId, indexSettings, new NIOFSDirectory(indexPath), new DummyShardLock(shardId))) {
            store.markStoreCorrupted(new IOException("fake ioexception"));
        }
    }

    private void checkNoValidShardCopy(String indexName, ShardId shardId) throws Exception {
        assertBusy(() -> {
            final ClusterAllocationExplanation explanation = client().admin()
                .cluster()
                .prepareAllocationExplain()
                .setIndex(indexName)
                .setShard(shardId.id())
                .setPrimary(true)
                .get()
                .getExplanation();

            final ShardAllocationDecision shardAllocationDecision = explanation.getShardAllocationDecision();
            assertThat(shardAllocationDecision.isDecisionTaken(), equalTo(true));
            assertThat(
                shardAllocationDecision.getAllocateDecision().getAllocationDecision(),
                equalTo(AllocationDecision.NO_VALID_SHARD_COPY)
            );
        });
    }

}
