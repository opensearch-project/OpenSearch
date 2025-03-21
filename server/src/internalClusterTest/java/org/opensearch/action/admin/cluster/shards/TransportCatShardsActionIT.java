/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.datastream.DataStreamTestCase;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.opensearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.search.SearchService.NO_TIMEOUT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 0, scope = OpenSearchIntegTestCase.Scope.TEST)
public class TransportCatShardsActionIT extends DataStreamTestCase {

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

    public void testListShardsWithHiddenIndex() throws Exception {
        final int numShards = 1;
        final int numReplicas = 1;
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(2);
        createIndex(
            "test-hidden-idx",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
                .build()
        );
        ensureGreen();

        // Verify result for a default query: "_list/shards"
        CatShardsRequest listShardsRequest = getListShardsTransportRequest(Strings.EMPTY_ARRAY, 100);
        ActionFuture<CatShardsResponse> listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertSingleIndexResponseShards(listShardsResponse.get(), "test-hidden-idx", 2, true);

        // Verify result when hidden index is explicitly queried: "_list/shards"
        listShardsRequest = getListShardsTransportRequest(new String[] { "test-hidden-idx" }, 100);
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertSingleIndexResponseShards(listShardsResponse.get(), "test-hidden-idx", 2, true);

        // Verify result when hidden index is queried with wildcard: "_list/shards*"
        // Since the ClusterStateAction underneath is invoked with lenientExpandOpen IndicesOptions,
        // Wildcards for hidden indices should not get resolved.
        listShardsRequest = getListShardsTransportRequest(new String[] { "test-hidden-idx*" }, 100);
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertEquals(0, listShardsResponse.get().getResponseShards().size());
        assertSingleIndexResponseShards(listShardsResponse.get(), "test-hidden-idx", 0, false);
    }

    public void testListShardsWithClosedIndex() throws Exception {
        final int numShards = 1;
        final int numReplicas = 1;
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(2);
        createIndex(
            "test-closed-idx",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .build()
        );
        ensureGreen();

        // close index "test-closed-idx"
        client().admin().indices().close(Requests.closeIndexRequest("test-closed-idx")).get();
        ensureGreen();

        // Verify result for a default query: "_list/shards"
        CatShardsRequest listShardsRequest = getListShardsTransportRequest(Strings.EMPTY_ARRAY, 100);
        ActionFuture<CatShardsResponse> listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertSingleIndexResponseShards(listShardsResponse.get(), "test-closed-idx", 2, false);

        // Verify result when closed index is explicitly queried: "_list/shards"
        listShardsRequest = getListShardsTransportRequest(new String[] { "test-closed-idx" }, 100);
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertSingleIndexResponseShards(listShardsResponse.get(), "test-closed-idx", 2, false);

        // Verify result when closed index is queried with wildcard: "_list/shards*"
        // Since the ClusterStateAction underneath is invoked with lenientExpandOpen IndicesOptions,
        // Wildcards for closed indices should not get resolved.
        listShardsRequest = getListShardsTransportRequest(new String[] { "test-closed-idx*" }, 100);
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertSingleIndexResponseShards(listShardsResponse.get(), "test-closed-idx", 0, false);
    }

    public void testListShardsWithClosedAndHiddenIndices() throws InterruptedException, ExecutionException {
        final int numIndices = 4;
        final int numShards = 1;
        final int numReplicas = 2;
        final int pageSize = 100;
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
        // close index "test-closed-idx"
        client().admin().indices().close(Requests.closeIndexRequest("test-closed-idx")).get();
        ensureGreen();

        // Verifying response for default queries: /_list/shards
        // all the shards should be part of response, however stats should not be displayed for closed index
        CatShardsRequest listShardsRequest = getListShardsTransportRequest(Strings.EMPTY_ARRAY, pageSize);
        ActionFuture<CatShardsResponse> listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertTrue(listShardsResponse.get().getResponseShards().stream().anyMatch(shard -> shard.getIndexName().equals("test-closed-idx")));
        assertTrue(listShardsResponse.get().getResponseShards().stream().anyMatch(shard -> shard.getIndexName().equals("test-hidden-idx")));
        assertEquals(numIndices * numShards * (numReplicas + 1), listShardsResponse.get().getResponseShards().size());
        assertFalse(
            Arrays.stream(listShardsResponse.get().getIndicesStatsResponse().getShards())
                .anyMatch(shardStats -> shardStats.getShardRouting().getIndexName().equals("test-closed-idx"))
        );
        assertEquals(
            (numIndices - 1) * numShards * (numReplicas + 1),
            listShardsResponse.get().getIndicesStatsResponse().getShards().length
        );

        // Verifying responses when hidden indices are explicitly queried: /_list/shards/test-hidden-idx
        // Shards for hidden index should appear in response along with stats
        listShardsRequest.setIndices(List.of("test-hidden-idx").toArray(new String[0]));
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertTrue(listShardsResponse.get().getResponseShards().stream().allMatch(shard -> shard.getIndexName().equals("test-hidden-idx")));
        assertTrue(
            Arrays.stream(listShardsResponse.get().getIndicesStatsResponse().getShards())
                .allMatch(shardStats -> shardStats.getShardRouting().getIndexName().equals("test-hidden-idx"))
        );
        assertEquals(
            listShardsResponse.get().getResponseShards().size(),
            listShardsResponse.get().getIndicesStatsResponse().getShards().length
        );

        // Verifying responses when hidden indices are queried with wildcards: /_list/shards/test-hidden-idx*
        // Shards for hidden index should not appear in response with stats.
        listShardsRequest.setIndices(List.of("test-hidden-idx*").toArray(new String[0]));
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertEquals(0, listShardsResponse.get().getResponseShards().size());
        assertEquals(0, listShardsResponse.get().getIndicesStatsResponse().getShards().length);

        // Explicitly querying for closed index: /_list/shards/test-closed-idx
        // should output closed shards without stats.
        listShardsRequest.setIndices(List.of("test-closed-idx").toArray(new String[0]));
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertTrue(listShardsResponse.get().getResponseShards().stream().anyMatch(shard -> shard.getIndexName().equals("test-closed-idx")));
        assertEquals(0, listShardsResponse.get().getIndicesStatsResponse().getShards().length);

        // Querying for closed index with wildcards: /_list/shards/test-closed-idx*
        // should not output any closed shards.
        listShardsRequest.setIndices(List.of("test-closed-idx*").toArray(new String[0]));
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertEquals(0, listShardsResponse.get().getResponseShards().size());
        assertEquals(0, listShardsResponse.get().getIndicesStatsResponse().getShards().length);
    }

    public void testListShardsWithClosedIndicesAcrossPages() throws InterruptedException, ExecutionException {
        final int numIndices = 4;
        final int numShards = 1;
        final int numReplicas = 2;
        final int pageSize = numShards * (numReplicas + 1);
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(3);
        createIndex(
            "test-open-idx-1",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .build()
        );
        createIndex(
            "test-closed-idx-1",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .build()
        );
        createIndex(
            "test-open-idx-2",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .build()
        );
        createIndex(
            "test-closed-idx-2",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(IndexMetadata.SETTING_INDEX_HIDDEN, true)
                .build()
        );
        // close index "test-closed-idx-1"
        client().admin().indices().close(Requests.closeIndexRequest("test-closed-idx-1")).get();
        ensureGreen();
        // close index "test-closed-idx-2"
        client().admin().indices().close(Requests.closeIndexRequest("test-closed-idx-2")).get();
        ensureGreen();

        // Verifying response for default queries: /_list/shards
        List<ShardRouting> responseShardRouting = new ArrayList<>();
        List<ShardStats> responseShardStats = new ArrayList<>();
        String nextToken = null;
        CatShardsRequest listShardsRequest;
        ActionFuture<CatShardsResponse> listShardsResponse;
        do {
            listShardsRequest = getListShardsTransportRequest(Strings.EMPTY_ARRAY, nextToken, pageSize);
            listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
            nextToken = listShardsResponse.get().getPageToken().getNextToken();
            responseShardRouting.addAll(listShardsResponse.get().getResponseShards());
            responseShardStats.addAll(List.of(listShardsResponse.get().getIndicesStatsResponse().getShards()));
        } while (nextToken != null);

        assertTrue(responseShardRouting.stream().anyMatch(shard -> shard.getIndexName().equals("test-closed-idx-1")));
        assertTrue(responseShardRouting.stream().anyMatch(shard -> shard.getIndexName().equals("test-closed-idx-2")));
        assertEquals(numIndices * numShards * (numReplicas + 1), responseShardRouting.size());
        // ShardsStats should only appear for 2 open indices
        assertFalse(
            responseShardStats.stream().anyMatch(shardStats -> shardStats.getShardRouting().getIndexName().contains("test-closed-idx"))
        );
        assertEquals(2 * numShards * (numReplicas + 1), responseShardStats.size());
    }

    public void testListShardsWithDataStream() throws Exception {
        final int numDataNodes = 3;
        String dataStreamName = "logs-test";
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(numDataNodes);
        // Create an index template for data streams.
        createDataStreamIndexTemplate("data-stream-template", List.of("logs-*"));
        // Create data streams matching the "logs-*" index pattern.
        createDataStream(dataStreamName);
        ensureGreen();
        // Verifying default query's result. Data stream should have created a hidden backing index in the
        // background and all the corresponding shards should appear in the response along with stats.
        CatShardsRequest listShardsRequest = getListShardsTransportRequest(Strings.EMPTY_ARRAY, numDataNodes * numDataNodes);
        ActionFuture<CatShardsResponse> listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertSingleIndexResponseShards(listShardsResponse.get(), dataStreamName, numDataNodes + 1, true);
        // Verifying result when data stream is directly queried. Again, all the shards with stats should appear
        listShardsRequest = getListShardsTransportRequest(new String[] { dataStreamName }, numDataNodes * numDataNodes);
        listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertSingleIndexResponseShards(listShardsResponse.get(), dataStreamName, numDataNodes + 1, true);
    }

    public void testListShardsWithAliases() throws Exception {
        final int numShards = 1;
        final int numReplicas = 1;
        final String aliasName = "test-alias";
        internalCluster().startClusterManagerOnlyNodes(1);
        internalCluster().startDataOnlyNodes(3);
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

        // Point test alias to both the indices (one being hidden while the other is closed)
        final IndicesAliasesRequest request = new IndicesAliasesRequest().origin("allowed");
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("test-closed-idx").alias(aliasName));
        assertAcked(client().admin().indices().aliases(request).actionGet());

        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("test-hidden-idx").alias(aliasName));
        assertAcked(client().admin().indices().aliases(request).actionGet());

        // close index "test-closed-idx"
        client().admin().indices().close(Requests.closeIndexRequest("test-closed-idx")).get();
        ensureGreen();

        // Verifying result when an alias is explicitly queried.
        CatShardsRequest listShardsRequest = getListShardsTransportRequest(new String[] { aliasName }, 100);
        ActionFuture<CatShardsResponse> listShardsResponse = client().execute(CatShardsAction.INSTANCE, listShardsRequest);
        assertTrue(
            listShardsResponse.get()
                .getResponseShards()
                .stream()
                .allMatch(shard -> shard.getIndexName().equals("test-hidden-idx") || shard.getIndexName().equals("test-closed-idx"))
        );
        assertTrue(
            Arrays.stream(listShardsResponse.get().getIndicesStatsResponse().getShards())
                .allMatch(shardStats -> shardStats.getShardRouting().getIndexName().equals("test-hidden-idx"))
        );
        assertEquals(4, listShardsResponse.get().getResponseShards().size());
        assertEquals(2, listShardsResponse.get().getIndicesStatsResponse().getShards().length);
    }

    private void assertSingleIndexResponseShards(
        CatShardsResponse catShardsResponse,
        String indexNamePattern,
        final int totalNumShards,
        boolean shardStatsExist
    ) {
        assertTrue(catShardsResponse.getResponseShards().stream().allMatch(shard -> shard.getIndexName().contains(indexNamePattern)));
        assertEquals(totalNumShards, catShardsResponse.getResponseShards().size());
        if (shardStatsExist) {
            assertTrue(
                Arrays.stream(catShardsResponse.getIndicesStatsResponse().getShards())
                    .allMatch(shardStats -> shardStats.getShardRouting().getIndexName().contains(indexNamePattern))
            );
        }
        assertEquals(shardStatsExist ? totalNumShards : 0, catShardsResponse.getIndicesStatsResponse().getShards().length);
    }

    private CatShardsRequest getListShardsTransportRequest(String[] indices, final int pageSize) {
        return getListShardsTransportRequest(indices, null, pageSize);
    }

    private CatShardsRequest getListShardsTransportRequest(String[] indices, String nextToken, final int pageSize) {
        CatShardsRequest listShardsRequest = new CatShardsRequest();
        listShardsRequest.setCancelAfterTimeInterval(NO_TIMEOUT);
        listShardsRequest.setIndices(indices);
        listShardsRequest.setPageParams(new PageParams(nextToken, PageParams.PARAM_ASC_SORT_VALUE, pageSize));
        return listShardsRequest;
    }
}
