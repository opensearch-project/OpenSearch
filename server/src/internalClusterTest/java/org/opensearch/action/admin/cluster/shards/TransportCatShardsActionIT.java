/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.pagination.PageParams;
import org.opensearch.client.Requests;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

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

    public void testListShardsWithClosedAndHiddenIndices() throws InterruptedException, ExecutionException {
        final int numIndices = 3;
        final int numShards = 1;
        final int numReplicas = 2;
        final int pageSize = numIndices * numReplicas * (numShards + 1);
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(3);
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .build()
        );
        createIndex(
            "test-2",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .build()
        );
        createIndex(
            "test-closed-idx",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .build()
        );
        createIndex(
            "test-hidden-idx",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
                .build()
        );
        ensureGreen();
        // close index "test-closed-idx"
        client().admin().indices().close(Requests.closeIndexRequest("test-closed-idx")).get();

        // Verifying responses for default queries: /_cat/shards and /_list/shards
        CatShardsRequest shardsRequest = new CatShardsRequest();
        shardsRequest.setCancelAfterTimeInterval(NO_TIMEOUT);
        shardsRequest.setIndices(Strings.EMPTY_ARRAY);
        ActionFuture<CatShardsResponse> catShardsResponse = client().execute(CatShardsAction.INSTANCE, shardsRequest);
        assertTrue(catShardsResponse.get().getResponseShards().stream().anyMatch(shard -> shard.getIndexName().equals("test-closed-idx")));
        assertTrue(catShardsResponse.get().getResponseShards().stream().anyMatch(shard -> shard.getIndexName().equals("test-hidden-idx")));

        shardsRequest.setPageParams(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, pageSize));
        ActionFuture<CatShardsResponse> listShardsResponse = client().execute(CatShardsAction.INSTANCE, shardsRequest);
        assertTrue(listShardsResponse.get().getResponseShards().stream().anyMatch(shard -> shard.getIndexName().equals("test-closed-idx")));
        assertTrue(listShardsResponse.get().getResponseShards().stream().anyMatch(shard -> shard.getIndexName().equals("test-hidden-idx")));
        assertEquals(catShardsResponse.get().getResponseShards().size(), listShardsResponse.get().getResponseShards().size());
        assertEquals(
            catShardsResponse.get().getIndicesStatsResponse().getShards().length,
            listShardsResponse.get().getIndicesStatsResponse().getShards().length
        );

        // Verifying responses when hidden indices are explicitly queried: /_cat/shards/test-hidden-idx and /_list/shards/test-hidden-idx
        // Shards for hidden index should appear in response along with stats
        shardsRequest = new CatShardsRequest();
        shardsRequest.setCancelAfterTimeInterval(NO_TIMEOUT);
        shardsRequest.setIndices(List.of("test-hidden-idx").toArray(new String[0]));
        catShardsResponse = client().execute(CatShardsAction.INSTANCE, shardsRequest);
        assertTrue(catShardsResponse.get().getResponseShards().stream().allMatch(shard -> shard.getIndexName().equals("test-hidden-idx")));
        assertTrue(
            Arrays.stream(catShardsResponse.get().getIndicesStatsResponse().getShards())
                .allMatch(shardStats -> shardStats.getShardRouting().getIndexName().equals("test-hidden-idx"))
        );
        assertEquals(
            catShardsResponse.get().getResponseShards().size(),
            catShardsResponse.get().getIndicesStatsResponse().getShards().length
        );

        shardsRequest.setPageParams(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, pageSize));
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, shardsRequest);
        assertTrue(listShardsResponse.get().getResponseShards().stream().allMatch(shard -> shard.getIndexName().equals("test-hidden-idx")));
        assertTrue(
            Arrays.stream(listShardsResponse.get().getIndicesStatsResponse().getShards())
                .allMatch(shardStats -> shardStats.getShardRouting().getIndexName().equals("test-hidden-idx"))
        );
        assertEquals(
            listShardsResponse.get().getResponseShards().size(),
            listShardsResponse.get().getIndicesStatsResponse().getShards().length
        );

        // Verifying responses when hidden indices are queried with wildcards: /_cat/shards/test-hidden-idx* and
        // /_list/shards/test-hidden-idx*
        // Shards for hidden index should appear in response without stats
        shardsRequest = new CatShardsRequest();
        shardsRequest.setCancelAfterTimeInterval(NO_TIMEOUT);
        shardsRequest.setIndices(List.of("test-hidden-idx*").toArray(new String[0]));
        catShardsResponse = client().execute(CatShardsAction.INSTANCE, shardsRequest);
        assertTrue(catShardsResponse.get().getResponseShards().stream().allMatch(shard -> shard.getIndexName().equals("test-hidden-idx")));
        assertEquals(0, catShardsResponse.get().getIndicesStatsResponse().getShards().length);

        shardsRequest.setPageParams(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, pageSize));
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, shardsRequest);
        assertTrue(listShardsResponse.get().getResponseShards().stream().allMatch(shard -> shard.getIndexName().equals("test-hidden-idx")));
        assertEquals(0, listShardsResponse.get().getIndicesStatsResponse().getShards().length);

        // Explicitly querying for closed index: /_cat/shards/test-closed-idx and /_list/shards/test-closed-idx
        // /_cat/shards/test-closed-idx should result in IndexClosedException
        // while /_list/shards/test-closed-idx should output closed shards without stats.
        shardsRequest = new CatShardsRequest();
        shardsRequest.setCancelAfterTimeInterval(NO_TIMEOUT);
        shardsRequest.setIndices(List.of("test-closed-idx").toArray(new String[0]));
        try {
            catShardsResponse = client().execute(CatShardsAction.INSTANCE, shardsRequest);
            catShardsResponse.get();
            fail("Expected IndexClosedException");
        } catch (Exception exception) {
            assertTrue(exception.getMessage().contains("IndexClosedException"));
        }

        shardsRequest.setPageParams(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, pageSize));
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, shardsRequest);
        assertTrue(listShardsResponse.get().getResponseShards().stream().allMatch(shard -> shard.getIndexName().equals("test-closed-idx")));
        assertEquals(0, listShardsResponse.get().getIndicesStatsResponse().getShards().length);

        // Querying for closed index with wildcards: /_cat/shards/test-closed-idx and /_list/shards/test-closed-idx
        // Both the queries should return zero entries
        shardsRequest = new CatShardsRequest();
        shardsRequest.setCancelAfterTimeInterval(NO_TIMEOUT);
        shardsRequest.setIndices(List.of("test-closed-idx*").toArray(new String[0]));
        catShardsResponse = client().execute(CatShardsAction.INSTANCE, shardsRequest);
        assertEquals(0, catShardsResponse.get().getResponseShards().size());
        assertEquals(0, catShardsResponse.get().getIndicesStatsResponse().getShards().length);

        shardsRequest.setPageParams(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, pageSize));
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, shardsRequest);
        assertEquals(0, listShardsResponse.get().getResponseShards().size());
        assertEquals(0, listShardsResponse.get().getIndicesStatsResponse().getShards().length);
    }

}
