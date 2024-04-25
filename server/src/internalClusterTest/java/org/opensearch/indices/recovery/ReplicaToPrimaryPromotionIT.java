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

package org.opensearch.indices.recovery;

import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Locale;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 2)
public class ReplicaToPrimaryPromotionIT extends OpenSearchIntegTestCase {

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Override
    public boolean useRandomReplicationStrategy() {
        return true;
    }

    public void testPromoteReplicaToPrimary() throws Exception {
        final String indexName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int numOfDocs = scaledRandomIntBetween(0, 200);
        if (numOfDocs > 0) {
            try (BackgroundIndexer indexer = new BackgroundIndexer(indexName, "_doc", client(), numOfDocs)) {
                waitForDocs(numOfDocs, indexer);
            }
            refreshAndWaitForReplication(indexName);
        }

        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numOfDocs);
        ensureGreen(indexName);

        // sometimes test with a closed index
        final IndexMetadata.State indexState = randomFrom(IndexMetadata.State.OPEN, IndexMetadata.State.CLOSE);
        if (indexState == IndexMetadata.State.CLOSE) {
            CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose(indexName).get();
            assertThat("close index not acked - " + closeIndexResponse, closeIndexResponse.isAcknowledged(), equalTo(true));
            ensureGreen(indexName);
        }

        // pick up a data node that contains a random primary shard
        ClusterState state = client(internalCluster().getClusterManagerName()).admin().cluster().prepareState().get().getState();
        final int numShards = state.metadata().index(indexName).getNumberOfShards();
        final ShardRouting primaryShard = state.routingTable().index(indexName).shard(randomIntBetween(0, numShards - 1)).primaryShard();
        final DiscoveryNode randomNode = state.nodes().resolveNode(primaryShard.currentNodeId());

        // stop the random data node, all remaining shards are promoted to primaries
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(randomNode.getName()));
        ensureYellowAndNoInitializingShards(indexName);

        state = client(internalCluster().getClusterManagerName()).admin().cluster().prepareState().get().getState();
        for (IndexShardRoutingTable shardRoutingTable : state.routingTable().index(indexName)) {
            for (ShardRouting shardRouting : shardRoutingTable.activeShards()) {
                assertThat(shardRouting + " should be promoted as a primary", shardRouting.primary(), is(true));
            }
        }

        if (indexState == IndexMetadata.State.CLOSE) {
            assertAcked(client().admin().indices().prepareOpen(indexName));
            ensureYellowAndNoInitializingShards(indexName);
        }
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), numOfDocs);
    }
}
