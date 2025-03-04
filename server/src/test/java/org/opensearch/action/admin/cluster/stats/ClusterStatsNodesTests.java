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

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodeStatsTests;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.flush.FlushStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.shard.IndexingStats;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.StoreStats;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.opensearch.core.xcontent.XContentHelper.toXContent;
import static org.hamcrest.Matchers.equalTo;

public class ClusterStatsNodesTests extends OpenSearchTestCase {

    /**
     * Test that empty transport/http types are not printed out as part
     * of the cluster stats xcontent output.
     */
    public void testNetworkTypesToXContent() throws Exception {
        ClusterStatsNodes.NetworkTypes stats = new ClusterStatsNodes.NetworkTypes(emptyList());
        assertEquals(
            "{\"transport_types\":{},\"http_types\":{}}",
            toXContent(stats, MediaTypeRegistry.JSON, randomBoolean()).utf8ToString()
        );

        List<NodeInfo> nodeInfos = singletonList(createNodeInfo("node_0", null, null));
        stats = new ClusterStatsNodes.NetworkTypes(nodeInfos);
        assertEquals(
            "{\"transport_types\":{},\"http_types\":{}}",
            toXContent(stats, MediaTypeRegistry.JSON, randomBoolean()).utf8ToString()
        );

        nodeInfos = Arrays.asList(
            createNodeInfo("node_1", "", ""),
            createNodeInfo("node_2", "custom", "custom"),
            createNodeInfo("node_3", null, "custom")
        );
        stats = new ClusterStatsNodes.NetworkTypes(nodeInfos);
        assertEquals(
            "{" + "\"transport_types\":{\"custom\":1}," + "\"http_types\":{\"custom\":2}" + "}",
            toXContent(stats, MediaTypeRegistry.JSON, randomBoolean()).utf8ToString()
        );
    }

    public void testIngestStats() throws Exception {
        NodeStats nodeStats = randomValueOtherThanMany(n -> n.getIngestStats() == null, () -> {
            try {
                return NodeStatsTests.createNodeStats();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        SortedMap<String, long[]> processorStats = new TreeMap<>();
        nodeStats.getIngestStats().getProcessorStats().values().forEach(stats -> {
            stats.forEach(stat -> {
                processorStats.compute(stat.getType(), (key, value) -> {
                    if (value == null) {
                        return new long[] {
                            stat.getStats().getCount(),
                            stat.getStats().getFailedCount(),
                            stat.getStats().getCurrent(),
                            stat.getStats().getTotalTimeInMillis() };
                    } else {
                        value[0] += stat.getStats().getCount();
                        value[1] += stat.getStats().getFailedCount();
                        value[2] += stat.getStats().getCurrent();
                        value[3] += stat.getStats().getTotalTimeInMillis();
                        return value;
                    }
                });
            });
        });

        ClusterStatsNodes.IngestStats stats = new ClusterStatsNodes.IngestStats(Collections.singletonList(nodeStats));
        assertThat(stats.pipelineCount, equalTo(nodeStats.getIngestStats().getProcessorStats().size()));
        String processorStatsString = "{";
        Iterator<Map.Entry<String, long[]>> iter = processorStats.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, long[]> entry = iter.next();
            long[] statValues = entry.getValue();
            long count = statValues[0];
            long failedCount = statValues[1];
            long current = statValues[2];
            long timeInMillis = statValues[3];
            processorStatsString += "\""
                + entry.getKey()
                + "\":{\"count\":"
                + count
                + ",\"failed\":"
                + failedCount
                + ",\"current\":"
                + current
                + ",\"time_in_millis\":"
                + timeInMillis
                + "}";
            if (iter.hasNext()) {
                processorStatsString += ",";
            }
        }
        processorStatsString += "}";
        assertThat(
            toXContent(stats, MediaTypeRegistry.JSON, false).utf8ToString(),
            equalTo(
                "{\"ingest\":{"
                    + "\"number_of_pipelines\":"
                    + stats.pipelineCount
                    + ","
                    + "\"processor_stats\":"
                    + processorStatsString
                    + "}}"
            )
        );
    }

    public void testMultiVersionScenarioWithAggregatedNodeLevelStats() {
        // Assuming the default behavior will be the type of response expected from a node of version prior to version containing
        // aggregated node level information
        int numberOfNodes = randomIntBetween(1, 4);
        Index testIndex = new Index("test-index", "_na_");

        List<ClusterStatsNodeResponse> defaultClusterStatsNodeResponses = new ArrayList<>();
        List<ClusterStatsNodeResponse> aggregatedNodeLevelClusterStatsNodeResponses = new ArrayList<>();

        for (int i = 0; i < numberOfNodes; i++) {
            DiscoveryNode node = new DiscoveryNode("node-" + i, buildNewFakeTransportAddress(), Version.CURRENT);
            CommonStats commonStats = createRandomCommonStats();
            ShardStats[] shardStats = createshardStats(node, testIndex, commonStats);
            ClusterStatsNodeResponse customClusterStatsResponse = createClusterStatsNodeResponse(node, shardStats, testIndex, true, false);
            ClusterStatsNodeResponse customNodeLevelAggregatedClusterStatsResponse = createClusterStatsNodeResponse(
                node,
                shardStats,
                testIndex,
                false,
                true
            );
            defaultClusterStatsNodeResponses.add(customClusterStatsResponse);
            aggregatedNodeLevelClusterStatsNodeResponses.add(customNodeLevelAggregatedClusterStatsResponse);
        }

        ClusterStatsIndices defaultClusterStatsIndices = new ClusterStatsIndices(defaultClusterStatsNodeResponses, null, null);
        ClusterStatsIndices aggregatedNodeLevelClusterStatsIndices = new ClusterStatsIndices(
            aggregatedNodeLevelClusterStatsNodeResponses,
            null,
            null
        );

        assertClusterStatsIndicesEqual(defaultClusterStatsIndices, aggregatedNodeLevelClusterStatsIndices);
    }

    public void assertClusterStatsIndicesEqual(ClusterStatsIndices first, ClusterStatsIndices second) {
        assertEquals(first.getIndexCount(), second.getIndexCount());

        assertEquals(first.getShards().getIndices(), second.getShards().getIndices());
        assertEquals(first.getShards().getTotal(), second.getShards().getTotal());
        assertEquals(first.getShards().getPrimaries(), second.getShards().getPrimaries());
        assertEquals(first.getShards().getMinIndexShards(), second.getShards().getMaxIndexShards());
        assertEquals(first.getShards().getMinIndexPrimaryShards(), second.getShards().getMinIndexPrimaryShards());

        // As AssertEquals with double is deprecated and can only be used to compare floating-point numbers
        assertTrue(first.getShards().getReplication() == second.getShards().getReplication());
        assertTrue(first.getShards().getAvgIndexShards() == second.getShards().getAvgIndexShards());
        assertTrue(first.getShards().getMaxIndexPrimaryShards() == second.getShards().getMaxIndexPrimaryShards());
        assertTrue(first.getShards().getAvgIndexPrimaryShards() == second.getShards().getAvgIndexPrimaryShards());
        assertTrue(first.getShards().getMinIndexReplication() == second.getShards().getMinIndexReplication());
        assertTrue(first.getShards().getAvgIndexReplication() == second.getShards().getAvgIndexReplication());
        assertTrue(first.getShards().getMaxIndexReplication() == second.getShards().getMaxIndexReplication());

        // Docs stats
        assertEquals(first.getDocs().getAverageSizeInBytes(), second.getDocs().getAverageSizeInBytes());
        assertEquals(first.getDocs().getDeleted(), second.getDocs().getDeleted());
        assertEquals(first.getDocs().getCount(), second.getDocs().getCount());
        assertEquals(first.getDocs().getTotalSizeInBytes(), second.getDocs().getTotalSizeInBytes());

        // Store Stats
        assertEquals(first.getStore().getSizeInBytes(), second.getStore().getSizeInBytes());
        assertEquals(first.getStore().getSize(), second.getStore().getSize());
        assertEquals(first.getStore().getReservedSize(), second.getStore().getReservedSize());

        // Query Cache
        assertEquals(first.getQueryCache().getCacheCount(), second.getQueryCache().getCacheCount());
        assertEquals(first.getQueryCache().getCacheSize(), second.getQueryCache().getCacheSize());
        assertEquals(first.getQueryCache().getEvictions(), second.getQueryCache().getEvictions());
        assertEquals(first.getQueryCache().getHitCount(), second.getQueryCache().getHitCount());
        assertEquals(first.getQueryCache().getTotalCount(), second.getQueryCache().getTotalCount());
        assertEquals(first.getQueryCache().getMissCount(), second.getQueryCache().getMissCount());
        assertEquals(first.getQueryCache().getMemorySize(), second.getQueryCache().getMemorySize());
        assertEquals(first.getQueryCache().getMemorySizeInBytes(), second.getQueryCache().getMemorySizeInBytes());

        // Completion Stats
        assertEquals(first.getCompletion().getSizeInBytes(), second.getCompletion().getSizeInBytes());
        assertEquals(first.getCompletion().getSize(), second.getCompletion().getSize());

        // Segment Stats
        assertEquals(first.getSegments().getBitsetMemory(), second.getSegments().getBitsetMemory());
        assertEquals(first.getSegments().getCount(), second.getSegments().getCount());
        assertEquals(first.getSegments().getBitsetMemoryInBytes(), second.getSegments().getBitsetMemoryInBytes());
        assertEquals(first.getSegments().getFileSizes(), second.getSegments().getFileSizes());
        assertEquals(first.getSegments().getIndexWriterMemoryInBytes(), second.getSegments().getIndexWriterMemoryInBytes());
        assertEquals(first.getSegments().getVersionMapMemory(), second.getSegments().getVersionMapMemory());
        assertEquals(first.getSegments().getVersionMapMemoryInBytes(), second.getSegments().getVersionMapMemoryInBytes());
    }

    public void testNodeIndexShardStatsSuccessfulSerializationDeserialization() throws IOException {
        Index testIndex = new Index("test-index", "_na_");

        DiscoveryNode node = new DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT);
        CommonStats commonStats = createRandomCommonStats();
        ShardStats[] shardStats = createshardStats(node, testIndex, commonStats);
        ClusterStatsNodeResponse aggregatedNodeLevelClusterStatsNodeResponse = createClusterStatsNodeResponse(
            node,
            shardStats,
            testIndex,
            false,
            true
        );

        BytesStreamOutput out = new BytesStreamOutput();
        aggregatedNodeLevelClusterStatsNodeResponse.writeTo(out);
        StreamInput in = out.bytes().streamInput();

        ClusterStatsNodeResponse newClusterStatsNodeRequest = new ClusterStatsNodeResponse(in);

        ClusterStatsIndices beforeSerialization = new ClusterStatsIndices(List.of(aggregatedNodeLevelClusterStatsNodeResponse), null, null);
        ClusterStatsIndices afterSerialization = new ClusterStatsIndices(List.of(newClusterStatsNodeRequest), null, null);

        assertClusterStatsIndicesEqual(beforeSerialization, afterSerialization);

    }

    private ClusterStatsNodeResponse createClusterStatsNodeResponse(
        DiscoveryNode node,
        ShardStats[] shardStats,
        Index index,
        boolean defaultBehavior,
        boolean aggregateNodeLevelStats
    ) {
        NodeInfo nodeInfo = new NodeInfo(
            Version.CURRENT,
            Build.CURRENT,
            node,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        NodeStats nodeStats = new NodeStats(
            node,
            randomNonNegativeLong(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        if (defaultBehavior) {
            return new ClusterStatsNodeResponse(node, null, nodeInfo, nodeStats, shardStats);
        } else {
            return new ClusterStatsNodeResponse(node, null, nodeInfo, nodeStats, shardStats, aggregateNodeLevelStats);
        }

    }

    private CommonStats createRandomCommonStats() {
        CommonStats commonStats = new CommonStats(CommonStatsFlags.NONE);
        commonStats.docs = new DocsStats(randomLongBetween(0, 10000), randomLongBetween(0, 100), randomLongBetween(0, 1000));
        commonStats.store = new StoreStats(randomLongBetween(0, 100), randomLongBetween(0, 1000));
        commonStats.indexing = new IndexingStats();
        commonStats.completion = new CompletionStats();
        commonStats.flush = new FlushStats(randomLongBetween(0, 100), randomLongBetween(0, 100), randomLongBetween(0, 100));
        commonStats.fieldData = new FieldDataStats(randomLongBetween(0, 100), randomLongBetween(0, 100), null);
        commonStats.queryCache = new QueryCacheStats(
            randomLongBetween(0, 100),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100)
        );
        commonStats.segments = new SegmentsStats();

        return commonStats;
    }

    private ShardStats[] createshardStats(DiscoveryNode localNode, Index index, CommonStats commonStats) {
        List<ShardStats> shardStatsList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ShardRoutingState shardRoutingState = ShardRoutingState.fromValue((byte) randomIntBetween(2, 3));
            ShardRouting shardRouting = TestShardRouting.newShardRouting(
                index.getName(),
                i,
                localNode.getId(),
                randomBoolean(),
                shardRoutingState
            );

            Path path = createTempDir().resolve("indices")
                .resolve(shardRouting.shardId().getIndex().getUUID())
                .resolve(String.valueOf(shardRouting.shardId().id()));

            ShardStats shardStats = new ShardStats(
                shardRouting,
                new ShardPath(false, path, path, shardRouting.shardId()),
                commonStats,
                null,
                null,
                null,
                null
            );
            shardStatsList.add(shardStats);
        }

        return shardStatsList.toArray(new ShardStats[0]);
    }

    private class MockShardStats extends ClusterStatsIndices.ShardStats {
        public boolean equals(ClusterStatsIndices.ShardStats shardStats) {
            return this.getIndices() == shardStats.getIndices()
                && this.getTotal() == shardStats.getTotal()
                && this.getPrimaries() == shardStats.getPrimaries()
                && this.getReplication() == shardStats.getReplication()
                && this.getMaxIndexShards() == shardStats.getMaxIndexShards()
                && this.getMinIndexShards() == shardStats.getMinIndexShards()
                && this.getAvgIndexShards() == shardStats.getAvgIndexShards()
                && this.getMaxIndexPrimaryShards() == shardStats.getMaxIndexPrimaryShards()
                && this.getMinIndexPrimaryShards() == shardStats.getMinIndexPrimaryShards()
                && this.getAvgIndexPrimaryShards() == shardStats.getAvgIndexPrimaryShards()
                && this.getMinIndexReplication() == shardStats.getMinIndexReplication()
                && this.getAvgIndexReplication() == shardStats.getAvgIndexReplication()
                && this.getMaxIndexReplication() == shardStats.getMaxIndexReplication();
        }
    }

    private static NodeInfo createNodeInfo(String nodeId, String transportType, String httpType) {
        Settings.Builder settings = Settings.builder();
        if (transportType != null) {
            settings.put(randomFrom(NetworkModule.TRANSPORT_TYPE_KEY, NetworkModule.TRANSPORT_TYPE_DEFAULT_KEY), transportType);
        }
        if (httpType != null) {
            settings.put(randomFrom(NetworkModule.HTTP_TYPE_KEY, NetworkModule.HTTP_TYPE_DEFAULT_KEY), httpType);
        }
        return new NodeInfo(
            null,
            null,
            new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), null),
            settings.build(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }
}
