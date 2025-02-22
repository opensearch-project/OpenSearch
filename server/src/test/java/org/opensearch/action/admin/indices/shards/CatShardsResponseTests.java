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

package org.opensearch.action.admin.indices.shards;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.shards.CatShardsResponse;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.pagination.PageToken;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;

public class CatShardsResponseTests extends OpenSearchTestCase {

    private static final String TEST_NODE_ID = "test_node_id";
    private final CatShardsResponse catShardsResponse = new CatShardsResponse();

    public void testGetAndSetNodes() {
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        catShardsResponse.setNodes(discoveryNodes);
        assertEquals(discoveryNodes, catShardsResponse.getNodes());
    }

    public void testGetAndSetIndicesStatsResponse() {
        final IndicesStatsResponse indicesStatsResponse = new IndicesStatsResponse(null, 0, 0, 0, null);
        catShardsResponse.setIndicesStatsResponse(indicesStatsResponse);

        assertEquals(indicesStatsResponse, catShardsResponse.getIndicesStatsResponse());
    }

    public void testSerialization() throws IOException {
        CatShardsResponse response = new CatShardsResponse();
        response.setIndicesStatsResponse(getIndicesStatsResponse());
        response.setNodes(getDiscoveryNodes());
        response.setPageToken(new PageToken("token", "test"));
        Version version = Version.CURRENT;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                CatShardsResponse deserialized = new CatShardsResponse(in);
                assertEquals(response.getPageToken(), deserialized.getPageToken());
                assertNotNull(deserialized.getNodes());
                assertEquals(1, deserialized.getNodes().getNodes().size());
                assertNotNull(deserialized.getNodes().getNodes().get(TEST_NODE_ID));
                assertNotNull(deserialized.getIndicesStatsResponse());
                assertEquals(
                    response.getIndicesStatsResponse().getShards().length,
                    deserialized.getIndicesStatsResponse().getShards().length
                );
                assertTrue(deserialized.getResponseShards().isEmpty());
            }
        }
    }

    public void testSerializationWithOnlyStatsAndNodesInResponse() throws IOException {
        CatShardsResponse response = new CatShardsResponse();
        response.setIndicesStatsResponse(getIndicesStatsResponse());
        response.setNodes(getDiscoveryNodes());
        Version version = Version.CURRENT;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                CatShardsResponse deserialized = new CatShardsResponse(in);
                assertEquals(response.getPageToken(), deserialized.getPageToken());
                assertNotNull(deserialized.getNodes());
                assertEquals(1, deserialized.getNodes().getNodes().size());
                assertNotNull(deserialized.getNodes().getNodes().get(TEST_NODE_ID));
                assertNotNull(deserialized.getIndicesStatsResponse());
                assertEquals(
                    response.getIndicesStatsResponse().getShards().length,
                    deserialized.getIndicesStatsResponse().getShards().length
                );
                assertTrue(deserialized.getResponseShards().isEmpty());
            }
        }
    }

    private DiscoveryNodes getDiscoveryNodes() throws UnknownHostException {
        DiscoveryNodes.Builder dnBuilder = new DiscoveryNodes.Builder();
        InetAddress inetAddress = InetAddress.getByAddress(new byte[] { (byte) 192, (byte) 168, (byte) 0, (byte) 1 });
        TransportAddress transportAddress = new TransportAddress(inetAddress, randomIntBetween(0, 65535));
        dnBuilder.add(new DiscoveryNode(TEST_NODE_ID, TEST_NODE_ID, transportAddress, emptyMap(), emptySet(), Version.CURRENT));
        return dnBuilder.build();
    }

    private IndicesStatsResponse getIndicesStatsResponse() {
        List<ShardStats> shards = new ArrayList<>();
        int noOfIndexes = randomIntBetween(2, 5);

        for (int indCnt = 0; indCnt < noOfIndexes; indCnt++) {
            Index index = createIndex(randomAlphaOfLength(9));
            int numShards = randomIntBetween(1, 5);
            for (int shardId = 0; shardId < numShards; shardId++) {
                ShardId shId = new ShardId(index, shardId);
                Path path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve(String.valueOf(shardId));
                ShardPath shardPath = new ShardPath(false, path, path, shId);
                ShardRouting routing = createShardRouting(shId, (shardId == 0));
                shards.add(new ShardStats(routing, shardPath, new CommonStats(), null, null, null, null));
            }
        }
        return new IndicesStatsResponse(shards.toArray(new ShardStats[0]), 0, 0, 0, null);
    }

    private ShardRouting createShardRouting(ShardId shardId, boolean isPrimary) {
        return TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(4), isPrimary, ShardRoutingState.STARTED);
    }

    private Index createIndex(String indexName) {
        return new Index(indexName, UUIDs.base64UUID());
    }
}
