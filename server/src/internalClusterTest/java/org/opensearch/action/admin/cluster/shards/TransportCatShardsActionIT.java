/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Priority;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.opensearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 0, scope = OpenSearchIntegTestCase.Scope.TEST)
public class TransportCatShardsActionIT extends OpenSearchIntegTestCase {

    public void testCatShardsWithSuccessResponse() {
        internalCluster().startClusterManagerOnlyNodes(1);
        List<String> nodes = internalCluster().startDataOnlyNodes(3);
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "60m")
                .build()
        );

        final CatShardsRequest shardsRequest = new CatShardsRequest();
        shardsRequest.setCancelAfterTimeInterval(timeValueMillis(1));
        shardsRequest.setIndices(Strings.EMPTY_ARRAY);
        client().execute(CatShardsAction.INSTANCE, shardsRequest, new ActionListener<CatShardsResponse>() {
            @Override
            public void onResponse(CatShardsResponse catShardsResponse) {
                ClusterStateResponse clusterStateResponse = catShardsResponse.getClusterStateResponse();
                IndicesStatsResponse indicesStatsResponse = catShardsResponse.getIndicesStatsResponse();
                for (ShardRouting shard : clusterStateResponse.getState().routingTable().allShards()) {
                    assertEquals("test", shard.getIndexName());
                    assertNotNull(indicesStatsResponse.asMap().get(shard));
                }
            }
            @Override
            public void onFailure(Exception e) {
            }
        });
    }

    public void testCatShardsWithFailure() throws Exception {
        Settings settings = Settings.builder().put("discovery.initial_state_timeout", "500ms").build();
        List<String> masterNodes = internalCluster().startClusterManagerOnlyNodes(1);
        List<String> dataNodes = internalCluster().startDataOnlyNodes(3);
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "60m")
                .build()
        );
        ensureGreen("test");

        final CatShardsRequest shardsRequest = new CatShardsRequest();
        shardsRequest.setCancelAfterTimeInterval(timeValueMillis(1));
        shardsRequest.setIndices(Strings.EMPTY_ARRAY);
        client().execute(CatShardsAction.INSTANCE, shardsRequest, new ActionListener<CatShardsResponse>() {
            @Override
            public void onResponse(CatShardsResponse catShardsResponse) {
                ClusterStateResponse clusterStateResponse = catShardsResponse.getClusterStateResponse();
                IndicesStatsResponse indicesStatsResponse = catShardsResponse.getIndicesStatsResponse();
                for (ShardRouting shard : clusterStateResponse.getState().routingTable().allShards()) {
                    assertEquals("test", shard.getIndexName());
//                    assertNotNull(indicesStatsResponse.asMap().get(shard));
                }
            }
            @Override
            public void onFailure(Exception e) {
            }
        });
    }

}
