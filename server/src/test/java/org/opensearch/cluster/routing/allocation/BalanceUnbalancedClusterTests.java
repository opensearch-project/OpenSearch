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

package org.opensearch.cluster.routing.allocation;

import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;

/**
 * see issue #9023
 */
public class BalanceUnbalancedClusterTests extends CatAllocationTestCase {

    @Override
    protected Path getCatPath() throws IOException {
        Path tmp = createTempDir();
        try (InputStream stream = Files.newInputStream(getDataPath("/org/opensearch/cluster/routing/issue_9023.zip"))) {
            TestUtil.unzip(stream, tmp);
        }
        return tmp.resolve("issue_9023");
    }

    @Override
    protected ClusterState allocateNew(ClusterState state) {
        String index = "tweets-2014-12-29:00";
        AllocationService strategy = createAllocationService(Settings.builder().build());
        Metadata metadata = Metadata.builder(state.metadata())
            .put(IndexMetadata.builder(index).settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(state.routingTable()).addAsNew(metadata.index(index)).build();

        ClusterState clusterState = ClusterState.builder(state).metadata(metadata).routingTable(initialRoutingTable).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        while (clusterState.routingTable().shardsWithState(INITIALIZING).isEmpty() == false) {
            clusterState = OpenSearchAllocationTestCase.startInitializingShardsAndReroute(strategy, clusterState);
        }
        Map<String, Integer> counts = new HashMap<>();
        for (IndexShardRoutingTable table : clusterState.routingTable().index(index)) {
            for (ShardRouting r : table) {
                String s = r.currentNodeId();
                Integer count = counts.get(s);
                if (count == null) {
                    count = 0;
                }
                count++;
                counts.put(s, count);
            }
        }
        for (Map.Entry<String, Integer> count : counts.entrySet()) {
            // we have 10 shards and 4 nodes so 2 nodes have 3 shards and 2 nodes have 2 shards
            assertTrue("Node: " + count.getKey() + " has shard mismatch: " + count.getValue(), count.getValue() >= 2);
            assertTrue("Node: " + count.getKey() + " has shard mismatch: " + count.getValue(), count.getValue() <= 3);

        }
        return clusterState;
    }

}
