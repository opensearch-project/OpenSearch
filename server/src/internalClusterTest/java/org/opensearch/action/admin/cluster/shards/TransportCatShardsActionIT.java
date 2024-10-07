/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.search.SearchService.NO_TIMEOUT;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 0, scope = OpenSearchIntegTestCase.Scope.TEST)
public class TransportCatShardsActionIT extends OpenSearchIntegTestCase {

    public void testCatShardsWithSuccessResponse() throws InterruptedException {
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
        ensureGreen("test");

        final CatShardsRequest shardsRequest = new CatShardsRequest();
        shardsRequest.setCancelAfterTimeInterval(NO_TIMEOUT);
        shardsRequest.setIndices(Strings.EMPTY_ARRAY);
        CountDownLatch latch = new CountDownLatch(1);
        client().execute(CatShardsAction.INSTANCE, shardsRequest, new ActionListener<CatShardsResponse>() {
            @Override
            public void onResponse(CatShardsResponse catShardsResponse) {
                List<ShardRouting> shardRoutings = catShardsResponse.getResponseShards();
                IndicesStatsResponse indicesStatsResponse = catShardsResponse.getIndicesStatsResponse();
                for (ShardRouting shard : shardRoutings) {
                    assertEquals("test", shard.getIndexName());
                    assertNotNull(indicesStatsResponse.asMap().get(shard));
                }
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail();
                latch.countDown();
            }
        });
        latch.await();
    }

    public void testCatShardsWithTimeoutException() throws IOException, AssertionError, InterruptedException {
        List<String> masterNodes = internalCluster().startClusterManagerOnlyNodes(1);
        List<String> nodes = internalCluster().startDataOnlyNodes(3);
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "60m")
                .build()
        );
        ensureGreen("test");

        Settings clusterManagerDataPathSettings = internalCluster().dataPathSettings(masterNodes.get(0));
        // Dropping master node to delay in cluster state call.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodes.get(0)));

        CountDownLatch latch = new CountDownLatch(2);
        new Thread(() -> {
            try {
                // Ensures the cancellation timeout expires.
                Thread.sleep(2000);
                // Starting master node to proceed in cluster state call.
                internalCluster().startClusterManagerOnlyNode(
                    Settings.builder().put("node.name", masterNodes.get(0)).put(clusterManagerDataPathSettings).build()
                );
                latch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        final CatShardsRequest shardsRequest = new CatShardsRequest();
        TimeValue timeoutInterval = timeValueMillis(1000);
        shardsRequest.setCancelAfterTimeInterval(timeoutInterval);
        shardsRequest.clusterManagerNodeTimeout(timeValueMillis(2500));
        shardsRequest.setIndices(Strings.EMPTY_ARRAY);
        client().execute(CatShardsAction.INSTANCE, shardsRequest, new ActionListener<CatShardsResponse>() {
            @Override
            public void onResponse(CatShardsResponse catShardsResponse) {
                // onResponse should not be called.
                latch.countDown();
                throw new AssertionError(
                    "The cat shards action is expected to fail with a TaskCancelledException, but it received a successful response instead."
                );
            }

            @Override
            public void onFailure(Exception e) {
                assertSame(e.getClass(), TaskCancelledException.class);
                assertEquals(e.getMessage(), "Cancellation timeout of " + timeoutInterval + " is expired");
                latch.countDown();
            }
        });
        latch.await();
    }

}
