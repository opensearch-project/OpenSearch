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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats.FileCacheStatsType;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class ClusterInfoTests extends OpenSearchTestCase {

    public void testSerialization() throws Exception {
        ClusterInfo clusterInfo = new ClusterInfo(
            randomDiskUsage(),
            randomDiskUsage(),
            randomShardSizes(),
            randomRoutingToDataPath(),
            randomReservedSpace(),
            randomFileCacheStats()
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
    }

    private static Map<String, DiskUsage> randomDiskUsage() {
        int numEntries = randomIntBetween(0, 128);
        final Map<String, DiskUsage> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(32);
            DiskUsage diskUsage = new DiskUsage(
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE)
            );
            builder.put(key, diskUsage);
        }
        return builder;
    }

    private static Map<String, AggregateFileCacheStats> randomFileCacheStats() {
        int numEntries = randomIntBetween(0, 16);
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
                    FileCacheStatsType.PINNED_FILE_STATS
                )
            );
            builder.put(key, fileCacheStats);
        }
        return builder;
    }

    private static Map<String, Long> randomShardSizes() {
        int numEntries = randomIntBetween(0, 128);
        final Map<String, Long> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(32);
            long shardSize = randomIntBetween(0, Integer.MAX_VALUE);
            builder.put(key, shardSize);
        }
        return builder;
    }

    private static Map<ShardRouting, String> randomRoutingToDataPath() {
        int numEntries = randomIntBetween(0, 128);
        final Map<ShardRouting, String> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            ShardId shardId = new ShardId(randomAlphaOfLength(32), randomAlphaOfLength(32), randomIntBetween(0, Integer.MAX_VALUE));
            ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, null, randomBoolean(), ShardRoutingState.UNASSIGNED);
            builder.put(shardRouting, randomAlphaOfLength(32));
        }
        return builder;
    }

    private static Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> randomReservedSpace() {
        int numEntries = randomIntBetween(0, 128);
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
