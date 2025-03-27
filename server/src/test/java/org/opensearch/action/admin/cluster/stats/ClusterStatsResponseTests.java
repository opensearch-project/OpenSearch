/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.IndexMetric;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.flush.FlushStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.shard.IndexingStats;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.StoreStats;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsInfo;
import org.opensearch.monitor.process.ProcessStats;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportInfo;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ClusterStatsResponseTests extends OpenSearchTestCase {

    public void testSerializationWithIndicesMappingAndAnalysisStats() throws Exception {
        List<ClusterStatsNodeResponse> defaultClusterStatsNodeResponses = new ArrayList<>();

        int numberOfNodes = randomIntBetween(1, 4);
        Index testIndex = new Index("test-index", "_na_");

        for (int i = 0; i < numberOfNodes; i++) {
            DiscoveryNode node = new DiscoveryNode("node-" + i, buildNewFakeTransportAddress(), Version.CURRENT);
            CommonStats commonStats = createRandomCommonStats();
            ShardStats[] shardStats = createShardStats(node, testIndex, commonStats);
            ClusterStatsNodeResponse customClusterStatsResponse = createClusterStatsNodeResponse(node, shardStats);
            defaultClusterStatsNodeResponses.add(customClusterStatsResponse);
        }
        ClusterStatsResponse clusterStatsResponse = new ClusterStatsResponse(
            1l,
            "UUID",
            new ClusterName("cluster_name"),
            defaultClusterStatsNodeResponses,
            List.of(),
            ClusterState.EMPTY_STATE,
            Set.of(ClusterStatsRequest.Metric.INDICES),
            Set.of(IndexMetric.MAPPINGS, IndexMetric.ANALYSIS)
        );
        BytesStreamOutput output = new BytesStreamOutput();
        clusterStatsResponse.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ClusterStatsResponse deserializedClusterStatsResponse = new ClusterStatsResponse(streamInput);
        assertEquals(clusterStatsResponse.timestamp, deserializedClusterStatsResponse.timestamp);
        assertEquals(clusterStatsResponse.status, deserializedClusterStatsResponse.status);
        assertEquals(clusterStatsResponse.clusterUUID, deserializedClusterStatsResponse.clusterUUID);
        assertNotNull(clusterStatsResponse.indicesStats);
        assertEquals(clusterStatsResponse.indicesStats.getMappings(), deserializedClusterStatsResponse.indicesStats.getMappings());
        assertEquals(clusterStatsResponse.indicesStats.getAnalysis(), deserializedClusterStatsResponse.indicesStats.getAnalysis());
    }

    public void testSerializationWithoutIndicesMappingAndAnalysisStats() throws Exception {
        List<ClusterStatsNodeResponse> defaultClusterStatsNodeResponses = new ArrayList<>();

        int numberOfNodes = randomIntBetween(1, 4);
        Index testIndex = new Index("test-index", "_na_");

        for (int i = 0; i < numberOfNodes; i++) {
            DiscoveryNode node = new DiscoveryNode("node-" + i, buildNewFakeTransportAddress(), Version.CURRENT);
            CommonStats commonStats = createRandomCommonStats();
            ShardStats[] shardStats = createShardStats(node, testIndex, commonStats);
            ClusterStatsNodeResponse customClusterStatsResponse = createClusterStatsNodeResponse(node, shardStats);
            defaultClusterStatsNodeResponses.add(customClusterStatsResponse);
        }
        ClusterStatsResponse clusterStatsResponse = new ClusterStatsResponse(
            1l,
            "UUID",
            new ClusterName("cluster_name"),
            defaultClusterStatsNodeResponses,
            List.of(),
            ClusterState.EMPTY_STATE,
            Set.of(ClusterStatsRequest.Metric.INDICES, ClusterStatsRequest.Metric.PROCESS, ClusterStatsRequest.Metric.JVM),
            Set.of(
                IndexMetric.DOCS,
                IndexMetric.STORE,
                IndexMetric.SEGMENTS,
                IndexMetric.QUERY_CACHE,
                IndexMetric.FIELDDATA,
                IndexMetric.COMPLETION
            )
        );
        BytesStreamOutput output = new BytesStreamOutput();
        clusterStatsResponse.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ClusterStatsResponse deserializedClusterStatsResponse = new ClusterStatsResponse(streamInput);
        assertEquals(clusterStatsResponse.timestamp, deserializedClusterStatsResponse.timestamp);
        assertEquals(clusterStatsResponse.status, deserializedClusterStatsResponse.status);
        assertEquals(clusterStatsResponse.clusterUUID, deserializedClusterStatsResponse.clusterUUID);
        assertNotNull(deserializedClusterStatsResponse.nodesStats);
        assertNotNull(deserializedClusterStatsResponse.nodesStats.getProcess());
        assertNotNull(deserializedClusterStatsResponse.nodesStats.getJvm());
        assertNotNull(deserializedClusterStatsResponse.indicesStats);
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getDocs());
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getStore());
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getSegments());
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getQueryCache());
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getFieldData());
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getCompletion());
        assertNull(deserializedClusterStatsResponse.indicesStats.getMappings());
        assertNull(deserializedClusterStatsResponse.indicesStats.getAnalysis());
    }

    private ClusterStatsNodeResponse createClusterStatsNodeResponse(DiscoveryNode node, ShardStats[] shardStats) throws IOException {
        JvmStats.GarbageCollector[] garbageCollectorsArray = new JvmStats.GarbageCollector[1];
        garbageCollectorsArray[0] = new JvmStats.GarbageCollector(
            randomAlphaOfLengthBetween(3, 10),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        JvmStats.GarbageCollectors garbageCollectors = new JvmStats.GarbageCollectors(garbageCollectorsArray);
        NodeInfo nodeInfo = new NodeInfo(
            Version.CURRENT,
            Build.CURRENT,
            node,
            Settings.EMPTY,
            new OsInfo(randomLong(), randomInt(), randomInt(), "name", "pretty_name", "arch", "version"),
            null,
            JvmInfo.jvmInfo(),
            null,
            new TransportInfo(
                new BoundTransportAddress(new TransportAddress[] { buildNewFakeTransportAddress() }, buildNewFakeTransportAddress()),
                null
            ),
            null,
            new PluginsAndModules(Collections.emptyList(), Collections.emptyList()),
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
            new ProcessStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                new ProcessStats.Cpu(randomShort(), randomNonNegativeLong()),
                new ProcessStats.Mem(randomNonNegativeLong())
            ),
            new JvmStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                new JvmStats.Mem(
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    Collections.emptyList()
                ),
                new JvmStats.Threads(randomIntBetween(1, 1000), randomIntBetween(1, 1000)),
                garbageCollectors,
                Collections.emptyList(),
                new JvmStats.Classes(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
            ),
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
        return new ClusterStatsNodeResponse(node, null, nodeInfo, nodeStats, shardStats);

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

    private ShardStats[] createShardStats(DiscoveryNode localNode, Index index, CommonStats commonStats) {
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

}
