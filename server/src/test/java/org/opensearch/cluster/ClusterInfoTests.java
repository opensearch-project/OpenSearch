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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats.FileCacheStatsType;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class ClusterInfoTests extends OpenSearchTestCase {

    public void testSerialization() throws Exception {
        ClusterInfo clusterInfo = new ClusterInfo(
            randomDiskUsage(randomIntBetween(0, 128)),
            randomDiskUsage(randomIntBetween(0, 128)),
            randomShardSizes(randomIntBetween(0, 128)),
            randomRoutingToDataPath(randomIntBetween(0, 18)),
            randomReservedSpace(randomIntBetween(0, 18)),
            randomFileCacheStats(randomIntBetween(0, 18)),
            randomNodeResourceUsageStats(randomIntBetween(0, 20))
        );
        BytesStreamOutput output = new BytesStreamOutput();
        clusterInfo.writeTo(output);

        ClusterInfo result = new ClusterInfo(output.bytes().streamInput());
        assertEquals(clusterInfo.getNodeLeastAvailableDiskUsages(), result.getNodeLeastAvailableDiskUsages());
        assertEquals(clusterInfo.getNodeMostAvailableDiskUsages(), result.getNodeMostAvailableDiskUsages());
        assertEquals(clusterInfo.shardSizes, result.shardSizes);
        assertEquals(clusterInfo.routingToDataPath, result.routingToDataPath);
        assertEquals(clusterInfo.reservedSpace, result.reservedSpace);
        assertEquals(clusterInfo.getNodeFileCacheStats().size(), result.getNodeFileCacheStats().size());
        assertEquals(clusterInfo.getNodeResourceUsageStats().toString(), result.getNodeResourceUsageStats().toString());
    }

    public void testToXContent() throws Exception {
        ClusterInfo clusterInfo = new ClusterInfo(
            randomDiskUsage(1),
            randomDiskUsage(1),
            randomShardSizes(1),
            randomRoutingToDataPath(1),
            randomReservedSpace(1),
            randomFileCacheStats(1),
            randomNodeResourceUsageStats(1)
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        clusterInfo.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {

            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());

            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());

            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            String nodeId = parser.currentName();

            // Verify node1 content
            parser.nextToken();
            Map<String, Object> node1 = parser.map();
            assertNotNull(node1.get("least_available"));
            assertNotNull(node1.get("most_available"));
            assertNotNull(node1.get("node_resource_usage_stats"));

            parser.nextToken();
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("shard_sizes", parser.currentName());

            // Verify shard_sizes
            parser.nextToken();
            Map<String, Object> shardSizesMap = parser.map();

            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("shard_paths", parser.currentName());

            // Verify shard_paths
            parser.nextToken();
            Map<String, Object> shardPathsMap = parser.map();

            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("reserved_sizes", parser.currentName());

            // Verify reserved_sizes
            assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            Map<String, Object> reservedSizeEntry = parser.map();
            assertTrue(reservedSizeEntry.containsKey("node_id"));
            assertTrue(reservedSizeEntry.containsKey("path"));

            assertEquals(XContentParser.Token.END_ARRAY, parser.nextToken());

            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        }
    }

    private static Map<String, DiskUsage> randomDiskUsage(int numEntries) {
        final Map<String, DiskUsage> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(32);
            long totalBytes = randomIntBetween(0, Integer.MAX_VALUE);
            DiskUsage diskUsage = new DiskUsage(
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                totalBytes,
                randomLongBetween(0, totalBytes)
            );
            builder.put(key, diskUsage);
        }
        return builder;
    }

    private static Map<String, NodeResourceUsageStats> randomNodeResourceUsageStats(int numEntries) {
        final Map<String, NodeResourceUsageStats> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(32);
            NodeResourceUsageStats nodeResourceUsageStats = new NodeResourceUsageStats(
                randomAlphaOfLength(4),
                randomLong(),
                randomDoubleBetween(0, 100, false),
                randomDoubleBetween(0, 100, false),
                null
            );
            builder.put(key, nodeResourceUsageStats);
        }
        return builder;
    }

    private static Map<String, AggregateFileCacheStats> randomFileCacheStats(int numEntries) {
        final Map<String, AggregateFileCacheStats> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(16);
            AggregateFileCacheStats fileCacheStats = new AggregateFileCacheStats(
                randomLong(),
                new FileCacheStats(
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    FileCacheStatsType.OVER_ALL_STATS
                ),
                new FileCacheStats(
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    FileCacheStatsType.FULL_FILE_STATS
                ),
                new FileCacheStats(
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    FileCacheStatsType.BLOCK_FILE_STATS
                ),
                new FileCacheStats(
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    randomLong(),
                    FileCacheStatsType.PINNED_FILE_STATS
                )
            );
            builder.put(key, fileCacheStats);
        }
        return builder;
    }

    private static Map<String, Long> randomShardSizes(int numEntries) {
        final Map<String, Long> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(32);
            long shardSize = randomIntBetween(0, Integer.MAX_VALUE);
            builder.put(key, shardSize);
        }
        return builder;
    }

    private static Map<ShardRouting, String> randomRoutingToDataPath(int numEntries) {
        final Map<ShardRouting, String> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            ShardId shardId = new ShardId(randomAlphaOfLength(32), randomAlphaOfLength(32), randomIntBetween(0, Integer.MAX_VALUE));
            ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, null, randomBoolean(), ShardRoutingState.UNASSIGNED);
            builder.put(shardRouting, randomAlphaOfLength(32));
        }
        return builder;
    }

    private static Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> randomReservedSpace(int numEntries) {
        final Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            final ClusterInfo.NodeAndPath key = new ClusterInfo.NodeAndPath(randomAlphaOfLength(10), randomAlphaOfLength(10));
            final ClusterInfo.ReservedSpace.Builder valueBuilder = new ClusterInfo.ReservedSpace.Builder();
            for (int j = between(0, 10); j > 0; j--) {
                ShardId shardId = new ShardId(randomAlphaOfLength(32), randomAlphaOfLength(32), randomIntBetween(0, Integer.MAX_VALUE));
                valueBuilder.add(shardId, between(0, Integer.MAX_VALUE));
            }
            builder.put(key, valueBuilder.build());
        }
        return builder;
    }

}
