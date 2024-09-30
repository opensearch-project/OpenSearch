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

package org.opensearch.rest.action.cat;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.Table;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.rest.pagination.PageToken;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestShardsActionTests extends OpenSearchTestCase {

    private final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
    private List<ShardRouting> shardRoutings = new ArrayList<>();
    private Map<ShardRouting, ShardStats> shardStatsMap = new HashMap<>();
    private ClusterStateResponse state;
    private IndicesStatsResponse stats;

    @Before
    public void setup() {
        final int numShards = randomIntBetween(1, 5);
        long numDocs = randomLongBetween(0, 10000);
        long numDeletedDocs = randomLongBetween(0, 100);

        String index = "index";
        for (int i = 0; i < numShards; i++) {
            ShardRoutingState shardRoutingState = ShardRoutingState.fromValue((byte) randomIntBetween(2, 3));
            ShardRouting shardRouting = TestShardRouting.newShardRouting(index, i, localNode.getId(), randomBoolean(), shardRoutingState);
            Path path = createTempDir().resolve("indices")
                .resolve(shardRouting.shardId().getIndex().getUUID())
                .resolve(String.valueOf(shardRouting.shardId().id()));
            CommonStats commonStats = new CommonStats();
            commonStats.docs = new DocsStats(numDocs, numDeletedDocs, 0);
            ShardStats shardStats = new ShardStats(
                shardRouting,
                new ShardPath(false, path, path, shardRouting.shardId()),
                commonStats,
                null,
                null,
                null
            );
            shardStatsMap.put(shardRouting, shardStats);
            shardRoutings.add(shardRouting);
        }

        IndexStats indexStats = mock(IndexStats.class);
        when(indexStats.getPrimaries()).thenReturn(new CommonStats());
        when(indexStats.getTotal()).thenReturn(new CommonStats());

        stats = mock(IndicesStatsResponse.class);
        when(stats.asMap()).thenReturn(shardStatsMap);

        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.get(localNode.getId())).thenReturn(localNode);

        state = mock(ClusterStateResponse.class);
        RoutingTable routingTable = mock(RoutingTable.class);
        when(routingTable.allShards()).thenReturn(shardRoutings);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.routingTable()).thenReturn(routingTable);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(state.getState()).thenReturn(clusterState);
    }

    public void testBuildTable() {
        final RestShardsAction action = new RestShardsAction();
        final Table table = action.buildTable(new FakeRestRequest(), state, stats, state.getState().routingTable().allShards(), null);
        assertTable(table);
    }

    public void testBuildTableWithPageToken() {
        final RestShardsAction action = new RestShardsAction();
        final Table table = action.buildTable(
            new FakeRestRequest(),
            state,
            stats,
            state.getState().routingTable().allShards(),
            new PageToken("foo", "test")
        );
        assertTable(table);
        assertNotNull(table.getPageToken());
        assertEquals("foo", table.getPageToken().getNextToken());
        assertEquals("test", table.getPageToken().getPaginatedEntity());
    }

    private void assertTable(Table table) {
        // now, verify the table is correct
        List<Table.Cell> headers = table.getHeaders();
        assertThat(headers.get(0).value, equalTo("index"));
        assertThat(headers.get(1).value, equalTo("shard"));
        assertThat(headers.get(2).value, equalTo("prirep"));
        assertThat(headers.get(3).value, equalTo("state"));
        assertThat(headers.get(4).value, equalTo("docs"));
        assertThat(headers.get(5).value, equalTo("store"));
        assertThat(headers.get(6).value, equalTo("ip"));
        assertThat(headers.get(7).value, equalTo("id"));
        assertThat(headers.get(8).value, equalTo("node"));
        assertThat(headers.get(79).value, equalTo("docs.deleted"));

        final List<List<Table.Cell>> rows = table.getRows();
        assertThat(rows.size(), equalTo(shardRoutings.size()));

        Iterator<ShardRouting> shardRoutingsIt = shardRoutings.iterator();
        for (final List<Table.Cell> row : rows) {
            ShardRouting shardRouting = shardRoutingsIt.next();
            ShardStats shardStats = shardStatsMap.get(shardRouting);
            assertThat(row.get(0).value, equalTo(shardRouting.getIndexName()));
            assertThat(row.get(1).value, equalTo(shardRouting.getId()));
            assertThat(row.get(2).value, equalTo(shardRouting.primary() ? "p" : "r"));
            assertThat(row.get(3).value, equalTo(shardRouting.state()));
            assertThat(row.get(4).value, equalTo(shardStats.getStats().getDocs().getCount()));
            assertThat(row.get(6).value, equalTo(localNode.getHostAddress()));
            assertThat(row.get(7).value, equalTo(localNode.getId()));
            assertThat(row.get(77).value, equalTo(shardStats.getDataPath()));
            assertThat(row.get(78).value, equalTo(shardStats.getStatePath()));
            assertThat(row.get(79).value, equalTo(shardStats.getStats().getDocs().getDeleted()));
        }
    }
}
