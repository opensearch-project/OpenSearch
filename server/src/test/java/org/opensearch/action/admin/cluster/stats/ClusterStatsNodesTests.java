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
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
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

    public void testMultiVersionScenario() {
        // Assuming the default behavior will be the type of response expected from a node of version prior to version containing optimized
        // output
        int numberOfNodes = randomIntBetween(1, 4);
        Index testIndex = new Index("test-index", "_na_");

        List<ClusterStatsNodeResponse> defaultClusterStatsNodeResponses = new ArrayList<>();
        List<ClusterStatsNodeResponse> optimizedClusterStatsNodeResponses = new ArrayList<>();

        boolean optimiseClusterStats = randomBoolean();

        for (int i = 0; i < numberOfNodes; i++) {
            DiscoveryNode node = new DiscoveryNode("node-" + i, buildNewFakeTransportAddress(), Version.CURRENT);
            CommonStats commonStats = createRandomCommonStats();
            ShardStats[] shardStats = createshardStats(node, testIndex, commonStats);
            ClusterStatsNodeResponse customClusterStatsResponse = createClusterStatsNodeResponse(node, shardStats, testIndex, true, false);
            ClusterStatsNodeResponse customOptimizedClusterStatsResponse = createClusterStatsNodeResponse(
                node,
                shardStats,
                testIndex,
                false,
                optimiseClusterStats
            );
            defaultClusterStatsNodeResponses.add(customClusterStatsResponse);
            optimizedClusterStatsNodeResponses.add(customOptimizedClusterStatsResponse);
        }

        ClusterStatsIndices defaultClusterStatsIndices = new ClusterStatsIndices(defaultClusterStatsNodeResponses, null, null);
        ClusterStatsIndices optimzedClusterStatsIndices = new ClusterStatsIndices(optimizedClusterStatsNodeResponses, null, null);

        assertEquals(defaultClusterStatsIndices.getIndexCount(), optimzedClusterStatsIndices.getIndexCount());

        assertEquals(defaultClusterStatsIndices.getShards().getIndices(), optimzedClusterStatsIndices.getShards().getIndices());
        assertEquals(defaultClusterStatsIndices.getShards().getTotal(), optimzedClusterStatsIndices.getShards().getTotal());
        assertEquals(defaultClusterStatsIndices.getShards().getPrimaries(), optimzedClusterStatsIndices.getShards().getPrimaries());
        assertEquals(
            defaultClusterStatsIndices.getShards().getMinIndexShards(),
            optimzedClusterStatsIndices.getShards().getMaxIndexShards()
        );
        assertEquals(
            defaultClusterStatsIndices.getShards().getMinIndexPrimaryShards(),
            optimzedClusterStatsIndices.getShards().getMinIndexPrimaryShards()
        );

        // As AssertEquals with double is deprecated and can only be used to compare floating-point numbers
        assertTrue(defaultClusterStatsIndices.getShards().getReplication() == optimzedClusterStatsIndices.getShards().getReplication());
        assertTrue(
            defaultClusterStatsIndices.getShards().getAvgIndexShards() == optimzedClusterStatsIndices.getShards().getAvgIndexShards()
        );
        assertTrue(
            defaultClusterStatsIndices.getShards().getMaxIndexPrimaryShards() == optimzedClusterStatsIndices.getShards()
                .getMaxIndexPrimaryShards()
        );
        assertTrue(
            defaultClusterStatsIndices.getShards().getAvgIndexPrimaryShards() == optimzedClusterStatsIndices.getShards()
                .getAvgIndexPrimaryShards()
        );
        assertTrue(
            defaultClusterStatsIndices.getShards().getMinIndexReplication() == optimzedClusterStatsIndices.getShards()
                .getMinIndexReplication()
        );
        assertTrue(
            defaultClusterStatsIndices.getShards().getAvgIndexReplication() == optimzedClusterStatsIndices.getShards()
                .getAvgIndexReplication()
        );
        assertTrue(
            defaultClusterStatsIndices.getShards().getMaxIndexReplication() == optimzedClusterStatsIndices.getShards()
                .getMaxIndexReplication()
        );

        // Docs stats
        assertEquals(
            defaultClusterStatsIndices.getDocs().getAverageSizeInBytes(),
            optimzedClusterStatsIndices.getDocs().getAverageSizeInBytes()
        );
        assertEquals(defaultClusterStatsIndices.getDocs().getDeleted(), optimzedClusterStatsIndices.getDocs().getDeleted());
        assertEquals(defaultClusterStatsIndices.getDocs().getCount(), optimzedClusterStatsIndices.getDocs().getCount());
        assertEquals(
            defaultClusterStatsIndices.getDocs().getTotalSizeInBytes(),
            optimzedClusterStatsIndices.getDocs().getTotalSizeInBytes()
        );

        // Store Stats
        assertEquals(defaultClusterStatsIndices.getStore().getSizeInBytes(), optimzedClusterStatsIndices.getStore().getSizeInBytes());
        assertEquals(defaultClusterStatsIndices.getStore().getSize(), optimzedClusterStatsIndices.getStore().getSize());
        assertEquals(defaultClusterStatsIndices.getStore().getReservedSize(), optimzedClusterStatsIndices.getStore().getReservedSize());

        // Query Cache
        assertEquals(
            defaultClusterStatsIndices.getQueryCache().getCacheCount(),
            optimzedClusterStatsIndices.getQueryCache().getCacheCount()
        );
        assertEquals(defaultClusterStatsIndices.getQueryCache().getCacheSize(), optimzedClusterStatsIndices.getQueryCache().getCacheSize());
        assertEquals(defaultClusterStatsIndices.getQueryCache().getEvictions(), optimzedClusterStatsIndices.getQueryCache().getEvictions());
        assertEquals(defaultClusterStatsIndices.getQueryCache().getHitCount(), optimzedClusterStatsIndices.getQueryCache().getHitCount());
        assertEquals(
            defaultClusterStatsIndices.getQueryCache().getTotalCount(),
            optimzedClusterStatsIndices.getQueryCache().getTotalCount()
        );
        assertEquals(defaultClusterStatsIndices.getQueryCache().getMissCount(), optimzedClusterStatsIndices.getQueryCache().getMissCount());
        assertEquals(
            defaultClusterStatsIndices.getQueryCache().getMemorySize(),
            optimzedClusterStatsIndices.getQueryCache().getMemorySize()
        );
        assertEquals(
            defaultClusterStatsIndices.getQueryCache().getMemorySizeInBytes(),
            optimzedClusterStatsIndices.getQueryCache().getMemorySizeInBytes()
        );

        // Completion Stats
        assertEquals(
            defaultClusterStatsIndices.getCompletion().getSizeInBytes(),
            optimzedClusterStatsIndices.getCompletion().getSizeInBytes()
        );
        assertEquals(defaultClusterStatsIndices.getCompletion().getSize(), optimzedClusterStatsIndices.getCompletion().getSize());

        // Segment Stats
        assertEquals(
            defaultClusterStatsIndices.getSegments().getBitsetMemory(),
            optimzedClusterStatsIndices.getSegments().getBitsetMemory()
        );
        assertEquals(defaultClusterStatsIndices.getSegments().getCount(), optimzedClusterStatsIndices.getSegments().getCount());
        assertEquals(
            defaultClusterStatsIndices.getSegments().getBitsetMemoryInBytes(),
            optimzedClusterStatsIndices.getSegments().getBitsetMemoryInBytes()
        );
        assertEquals(defaultClusterStatsIndices.getSegments().getFileSizes(), optimzedClusterStatsIndices.getSegments().getFileSizes());
        assertEquals(
            defaultClusterStatsIndices.getSegments().getIndexWriterMemoryInBytes(),
            optimzedClusterStatsIndices.getSegments().getIndexWriterMemoryInBytes()
        );
        assertEquals(
            defaultClusterStatsIndices.getSegments().getVersionMapMemory(),
            optimzedClusterStatsIndices.getSegments().getVersionMapMemory()
        );
        assertEquals(
            defaultClusterStatsIndices.getSegments().getVersionMapMemoryInBytes(),
            optimzedClusterStatsIndices.getSegments().getVersionMapMemoryInBytes()
        );

    }

    private ClusterStatsNodeResponse createClusterStatsNodeResponse(
        DiscoveryNode node,
        ShardStats[] shardStats,
        Index index,
        boolean defaultBehavior,
        boolean optimized
    ) {
        if (defaultBehavior) {
            return new ClusterStatsNodeResponse(node, null, null, null, shardStats);
        } else {
            return new ClusterStatsNodeResponse(node, null, null, null, shardStats, optimized);
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
