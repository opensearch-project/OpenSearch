/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nodestats;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.shard.IndexingStats.Stats.DocStatusStats;
import org.opensearch.indices.NodeIndicesStats;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonMap;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class NodeStatsIT extends OpenSearchIntegTestCase {

    private final DocStatusStats expectedDocStatusStats = new DocStatusStats();
    private static final String FIELD = "dummy_field";
    private static final String VALUE = "dummy_value";
    private static final Map<String, Object> SOURCE = singletonMap(FIELD, VALUE);

    public void testNodeIndicesStatsDocStatusStatsIndexBulk() {
        {  // Testing Index
            final String INDEX = "test_index";
            final String ID = "id";
            {  // Testing Normal Index
                IndexResponse response = client().index(new IndexRequest(INDEX).id(ID).source(SOURCE)).actionGet();
                updateExpectedDocStatusCounter(response);

                MatcherAssert.assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
                assertDocStatusStats();
            }
            {  // Testing Missing Alias
                updateExpectedDocStatusCounter(
                    expectThrows(
                        IndexNotFoundException.class,
                        () -> client().index(new IndexRequest(INDEX).id("missing_alias").setRequireAlias(true).source(SOURCE)).actionGet()
                    )
                );
                assertDocStatusStats();
            }
            {
                // Test Missing Pipeline: Ingestion failure, not Indexing failure
                expectThrows(
                    IllegalArgumentException.class,
                    () -> client().index(new IndexRequest(INDEX).id("missing_pipeline").setPipeline("missing").source(SOURCE)).actionGet()
                );
                assertDocStatusStats();
            }
            {  // Testing Version Conflict
                final String docId = "version_conflict";

                updateExpectedDocStatusCounter(client().index(new IndexRequest(INDEX).id(docId).source(SOURCE)).actionGet());
                updateExpectedDocStatusCounter(
                    expectThrows(
                        VersionConflictEngineException.class,
                        () -> client().index(new IndexRequest(INDEX).id(docId).source(SOURCE).setIfSeqNo(1L).setIfPrimaryTerm(99L))
                            .actionGet()
                    )
                );
                assertDocStatusStats();
            }
        }
        {  // Testing Bulk
            final String INDEX = "bulk_index";

            int sizeOfIndexRequests = scaledRandomIntBetween(10, 20);
            int sizeOfDeleteRequests = scaledRandomIntBetween(5, sizeOfIndexRequests);
            int sizeOfNotFoundRequests = scaledRandomIntBetween(5, sizeOfIndexRequests);

            BulkRequest bulkRequest = new BulkRequest();

            for (int i = 0; i < sizeOfIndexRequests; ++i) {
                bulkRequest.add(new IndexRequest(INDEX).id(String.valueOf(i)).source(SOURCE));
            }

            BulkResponse response = client().bulk(bulkRequest).actionGet();

            MatcherAssert.assertThat(response.hasFailures(), equalTo(false));
            MatcherAssert.assertThat(response.getItems().length, equalTo(sizeOfIndexRequests));

            for (BulkItemResponse itemResponse : response.getItems()) {
                updateExpectedDocStatusCounter(itemResponse.getResponse());
            }

            refresh(INDEX);
            bulkRequest.requests().clear();

            for (int i = 0; i < sizeOfDeleteRequests; ++i) {
                bulkRequest.add(new DeleteRequest(INDEX, String.valueOf(i)));
            }
            for (int i = 0; i < sizeOfNotFoundRequests; ++i) {
                bulkRequest.add(new DeleteRequest(INDEX, String.valueOf(25 + i)));
            }

            response = client().bulk(bulkRequest).actionGet();

            MatcherAssert.assertThat(response.hasFailures(), equalTo(false));
            MatcherAssert.assertThat(response.getItems().length, equalTo(sizeOfDeleteRequests + sizeOfNotFoundRequests));

            for (BulkItemResponse itemResponse : response.getItems()) {
                updateExpectedDocStatusCounter(itemResponse.getResponse());
            }

            refresh(INDEX);
            assertDocStatusStats();
        }
    }

    public void testNodeIndicesStatsDocStatusStatsCreateDeleteUpdate() {
        {  // Testing Create
            final String INDEX = "create_index";
            final String ID = "id";
            {  // Testing Creation
                IndexResponse response = client().index(new IndexRequest(INDEX).id(ID).source(SOURCE).create(true)).actionGet();
                updateExpectedDocStatusCounter(response);

                MatcherAssert.assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
                assertDocStatusStats();
            }
            {  // Testing Version Conflict
                final String docId = "version_conflict";

                updateExpectedDocStatusCounter(client().index(new IndexRequest(INDEX).id(docId).source(SOURCE)).actionGet());
                updateExpectedDocStatusCounter(
                    expectThrows(
                        VersionConflictEngineException.class,
                        () -> client().index(new IndexRequest(INDEX).id(docId).source(SOURCE).create(true)).actionGet()
                    )
                );
                assertDocStatusStats();
            }
        }
        {  // Testing Delete
            final String INDEX = "delete_index";
            final String ID = "id";
            {  // Testing Deletion
                IndexResponse response = client().index(new IndexRequest(INDEX).id(ID).source(SOURCE)).actionGet();
                updateExpectedDocStatusCounter(response);

                DeleteResponse deleteResponse = client().delete(new DeleteRequest(INDEX, ID)).actionGet();
                updateExpectedDocStatusCounter(deleteResponse);

                MatcherAssert.assertThat(response.getSeqNo(), greaterThanOrEqualTo(0L));
                MatcherAssert.assertThat(deleteResponse.getResult(), equalTo(DocWriteResponse.Result.DELETED));
                assertDocStatusStats();
            }
            {  // Testing Non-Existing Doc
                updateExpectedDocStatusCounter(client().delete(new DeleteRequest(INDEX, "does_not_exist")).actionGet());
                assertDocStatusStats();
            }
            {  // Testing Version Conflict
                final String docId = "version_conflict";

                updateExpectedDocStatusCounter(client().index(new IndexRequest(INDEX).id(docId).source(SOURCE)).actionGet());
                updateExpectedDocStatusCounter(
                    expectThrows(
                        VersionConflictEngineException.class,
                        () -> client().delete(new DeleteRequest(INDEX, docId).setIfSeqNo(2L).setIfPrimaryTerm(99L)).actionGet()
                    )
                );

                assertDocStatusStats();
            }
        }
        {  // Testing Update
            final String INDEX = "update_index";
            final String ID = "id";
            {  // Testing Not Found
                updateExpectedDocStatusCounter(
                    expectThrows(
                        DocumentMissingException.class,
                        () -> client().update(new UpdateRequest(INDEX, ID).doc(SOURCE)).actionGet()
                    )
                );
                assertDocStatusStats();
            }
            {  // Testing NoOp Update
                updateExpectedDocStatusCounter(client().index(new IndexRequest(INDEX).id(ID).source(SOURCE)).actionGet());

                UpdateResponse response = client().update(new UpdateRequest(INDEX, ID).doc(SOURCE)).actionGet();
                updateExpectedDocStatusCounter(response);

                MatcherAssert.assertThat(response.getResult(), equalTo(DocWriteResponse.Result.NOOP));
                assertDocStatusStats();
            }
            {  // Testing Update
                final String UPDATED_VALUE = "updated_value";
                UpdateResponse response = client().update(new UpdateRequest(INDEX, ID).doc(singletonMap(FIELD, UPDATED_VALUE))).actionGet();
                updateExpectedDocStatusCounter(response);

                MatcherAssert.assertThat(response.getResult(), equalTo(DocWriteResponse.Result.UPDATED));
                assertDocStatusStats();
            }
            {  // Testing Missing Alias
                updateExpectedDocStatusCounter(
                    expectThrows(
                        IndexNotFoundException.class,
                        () -> client().update(new UpdateRequest(INDEX, ID).setRequireAlias(true).doc(new IndexRequest().source(SOURCE)))
                            .actionGet()
                    )
                );
                assertDocStatusStats();
            }
            {  // Testing Version Conflict
                final String docId = "version_conflict";

                updateExpectedDocStatusCounter(client().index(new IndexRequest(INDEX).id(docId).source(SOURCE)).actionGet());
                updateExpectedDocStatusCounter(
                    expectThrows(
                        VersionConflictEngineException.class,
                        () -> client().update(new UpdateRequest(INDEX, docId).doc(SOURCE).setIfSeqNo(2L).setIfPrimaryTerm(99L)).actionGet()
                    )
                );
                assertDocStatusStats();
            }
        }
    }

    public void testNodeIndicesStatsDocStatsWithAggregations() {
        {  // Testing Create
            final String INDEX = "create_index";
            final String ID = "id";
            DocStatusStats expectedDocStatusStats = new DocStatusStats();

            IndexResponse response = client().index(new IndexRequest(INDEX).id(ID).source(SOURCE).create(true)).actionGet();
            expectedDocStatusStats.inc(response.status());

            CommonStatsFlags commonStatsFlags = new CommonStatsFlags();
            commonStatsFlags.setIncludeIndicesStatsByLevel(true);

            DocStatusStats docStatusStats = client().admin()
                .cluster()
                .prepareNodesStats()
                .setIndices(commonStatsFlags)
                .execute()
                .actionGet()
                .getNodes()
                .get(0)
                .getIndices()
                .getIndexing()
                .getTotal()
                .getDocStatusStats();

            assertTrue(
                Arrays.equals(
                    docStatusStats.getDocStatusCounter(),
                    expectedDocStatusStats.getDocStatusCounter(),
                    Comparator.comparingLong(AtomicLong::longValue)
                )
            );
        }
    }

    /**
     * Default behavior - without consideration of request level param on level, the NodeStatsRequest always
     * returns ShardStats which is aggregated on the coordinator node when creating the XContent.
     */
    public void testNodeIndicesStatsXContentWithoutAggregationOnNodes() {
        List<String> testLevels = new ArrayList<>();
        testLevels.add("null");
        testLevels.add(NodeIndicesStats.StatsLevel.NODE.getRestName());
        testLevels.add(NodeIndicesStats.StatsLevel.INDICES.getRestName());
        testLevels.add(NodeIndicesStats.StatsLevel.SHARDS.getRestName());
        testLevels.add("unknown");

        internalCluster().startNode();
        ensureGreen();
        String indexName = "test1";
        assertAcked(
            prepareCreate(
                indexName,
                clusterService().state().getNodes().getSize(),
                Settings.builder().put("number_of_shards", 2).put("number_of_replicas", clusterService().state().getNodes().getSize() - 1)
            )
        );
        ensureGreen();
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();

        testLevels.forEach(testLevel -> {
            NodesStatsResponse response;
            if (!testLevel.equals("null")) {
                ArrayList<String> level_arg = new ArrayList<>();
                level_arg.add(testLevel);

                CommonStatsFlags commonStatsFlags = new CommonStatsFlags();
                commonStatsFlags.setLevels(level_arg.toArray(new String[0]));
                response = client().admin().cluster().prepareNodesStats().setIndices(commonStatsFlags).get();
            } else {
                response = client().admin().cluster().prepareNodesStats().get();
            }

            NodeStats nodeStats = response.getNodes().get(0);
            assertNotNull(nodeStats.getIndices().getShardStats(clusterState.metadata().index(indexName).getIndex()));
            try {
                // Without any param - default is level = nodes
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                builder = nodeStats.getIndices().toXContent(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();

                Map<String, Object> xContentMap = xContentBuilderToMap(builder);
                LinkedHashMap indicesStatsMap = (LinkedHashMap) xContentMap.get(NodeIndicesStats.StatsLevel.INDICES.getRestName());
                assertFalse(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.INDICES));
                assertFalse(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.SHARDS));

                // With param containing level as 'indices', the indices stats are returned
                builder = XContentFactory.jsonBuilder();
                builder.startObject();
                builder = nodeStats.getIndices()
                    .toXContent(
                        builder,
                        new ToXContent.MapParams(Collections.singletonMap("level", NodeIndicesStats.StatsLevel.INDICES.getRestName()))
                    );
                builder.endObject();

                xContentMap = xContentBuilderToMap(builder);
                indicesStatsMap = (LinkedHashMap) xContentMap.get(NodeIndicesStats.StatsLevel.INDICES.getRestName());
                assertTrue(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.INDICES.getRestName()));
                assertFalse(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.SHARDS.getRestName()));

                LinkedHashMap indexLevelStats = (LinkedHashMap) indicesStatsMap.get(NodeIndicesStats.StatsLevel.INDICES.getRestName());
                assertTrue(indexLevelStats.containsKey(indexName));

                // With param containing level as 'shards', the shard stats are returned
                builder = XContentFactory.jsonBuilder();
                builder.startObject();
                builder = nodeStats.getIndices()
                    .toXContent(
                        builder,
                        new ToXContent.MapParams(Collections.singletonMap("level", NodeIndicesStats.StatsLevel.SHARDS.getRestName()))
                    );
                builder.endObject();

                xContentMap = xContentBuilderToMap(builder);
                indicesStatsMap = (LinkedHashMap) xContentMap.get(NodeIndicesStats.StatsLevel.INDICES.getRestName());
                assertFalse(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.INDICES.getRestName()));
                assertTrue(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.SHARDS.getRestName()));

                LinkedHashMap shardLevelStats = (LinkedHashMap) indicesStatsMap.get(NodeIndicesStats.StatsLevel.SHARDS.getRestName());
                assertTrue(shardLevelStats.containsKey(indexName));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Aggregated behavior - to avoid unnecessary IO in the form of shard-stats when not required, we not honor the levels on the
     * individual data nodes instead and pre-compute information as required.
     */
    public void testNodeIndicesStatsXContentWithAggregationOnNodes() {
        List<MockStatsLevel> testLevels = new ArrayList<>();

        testLevels.add(MockStatsLevel.NULL);
        testLevels.add(MockStatsLevel.NODE);
        testLevels.add(MockStatsLevel.INDICES);
        testLevels.add(MockStatsLevel.SHARDS);

        internalCluster().startNode();
        ensureGreen();
        String indexName = "test1";
        assertAcked(
            prepareCreate(
                indexName,
                clusterService().state().getNodes().getSize(),
                Settings.builder().put("number_of_shards", 2).put("number_of_replicas", clusterService().state().getNodes().getSize() - 1)
            )
        );
        ensureGreen();

        testLevels.forEach(testLevel -> {
            NodesStatsResponse response;
            CommonStatsFlags commonStatsFlags = new CommonStatsFlags();
            commonStatsFlags.setIncludeIndicesStatsByLevel(true);
            if (!testLevel.equals(MockStatsLevel.NULL)) {
                ArrayList<String> level_arg = new ArrayList<>();
                level_arg.add(testLevel.getRestName());

                commonStatsFlags.setLevels(level_arg.toArray(new String[0]));
            }
            response = client().admin().cluster().prepareNodesStats().setIndices(commonStatsFlags).get();

            NodeStats nodeStats = response.getNodes().get(0);
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();

                builder.startObject();

                if (!testLevel.equals(MockStatsLevel.SHARDS)) {
                    final XContentBuilder failedBuilder = builder;
                    assertThrows(
                        "Expected shard stats in response for generating [SHARDS] field",
                        AssertionError.class,
                        () -> nodeStats.getIndices()
                            .toXContent(
                                failedBuilder,
                                new ToXContent.MapParams(
                                    Collections.singletonMap("level", NodeIndicesStats.StatsLevel.SHARDS.getRestName())
                                )
                            )
                    );
                } else {
                    builder = nodeStats.getIndices()
                        .toXContent(
                            builder,
                            new ToXContent.MapParams(Collections.singletonMap("level", NodeIndicesStats.StatsLevel.SHARDS.getRestName()))
                        );
                    builder.endObject();

                    Map<String, Object> xContentMap = xContentBuilderToMap(builder);
                    LinkedHashMap indicesStatsMap = (LinkedHashMap) xContentMap.get(NodeIndicesStats.StatsLevel.INDICES.getRestName());
                    LinkedHashMap indicesStats = (LinkedHashMap) indicesStatsMap.get(NodeIndicesStats.StatsLevel.INDICES.getRestName());
                    LinkedHashMap shardStats = (LinkedHashMap) indicesStatsMap.get(NodeIndicesStats.StatsLevel.SHARDS.getRestName());

                    assertFalse(shardStats.isEmpty());
                    assertNull(indicesStats);
                }

                builder = XContentFactory.jsonBuilder();
                builder.startObject();

                if (!(testLevel.equals(MockStatsLevel.SHARDS) || testLevel.equals(MockStatsLevel.INDICES))) {
                    final XContentBuilder failedBuilder = builder;
                    assertThrows(
                        "Expected shard stats or index stats in response for generating INDICES field",
                        AssertionError.class,
                        () -> nodeStats.getIndices()
                            .toXContent(
                                failedBuilder,
                                new ToXContent.MapParams(
                                    Collections.singletonMap("level", NodeIndicesStats.StatsLevel.INDICES.getRestName())
                                )
                            )
                    );
                } else {
                    builder = nodeStats.getIndices()
                        .toXContent(
                            builder,
                            new ToXContent.MapParams(Collections.singletonMap("level", NodeIndicesStats.StatsLevel.INDICES.getRestName()))
                        );
                    builder.endObject();

                    Map<String, Object> xContentMap = xContentBuilderToMap(builder);
                    LinkedHashMap indicesStatsMap = (LinkedHashMap) xContentMap.get(NodeIndicesStats.StatsLevel.INDICES.getRestName());
                    LinkedHashMap indicesStats = (LinkedHashMap) indicesStatsMap.get(NodeIndicesStats.StatsLevel.INDICES.getRestName());
                    LinkedHashMap shardStats = (LinkedHashMap) indicesStatsMap.get(NodeIndicesStats.StatsLevel.SHARDS.getRestName());

                    switch (testLevel) {
                        case SHARDS:
                        case INDICES:
                            assertNull(shardStats);
                            assertFalse(indicesStats.isEmpty());
                            break;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testNodeIndicesStatsUnknownLevelThrowsException() {
        MockStatsLevel testLevel = MockStatsLevel.UNKNOWN;
        internalCluster().startNode();
        ensureGreen();
        String indexName = "test1";
        assertAcked(
            prepareCreate(
                indexName,
                clusterService().state().getNodes().getSize(),
                Settings.builder().put("number_of_shards", 2).put("number_of_replicas", clusterService().state().getNodes().getSize() - 1)
            )
        );
        ensureGreen();

        NodesStatsResponse response;
        CommonStatsFlags commonStatsFlags = new CommonStatsFlags();
        commonStatsFlags.setIncludeIndicesStatsByLevel(true);
        ArrayList<String> level_arg = new ArrayList<>();
        level_arg.add(testLevel.getRestName());

        commonStatsFlags.setLevels(level_arg.toArray(new String[0]));
        response = client().admin().cluster().prepareNodesStats().setIndices(commonStatsFlags).get();

        assertTrue(response.hasFailures());
        assertEquals("Level provided is not supported by NodeIndicesStats", response.failures().get(0).getCause().getCause().getMessage());
    }

    private Map<String, Object> xContentBuilderToMap(XContentBuilder xContentBuilder) {
        return XContentHelper.convertToMap(BytesReference.bytes(xContentBuilder), true, xContentBuilder.contentType()).v2();
    }

    private void assertDocStatusStats() {
        DocStatusStats docStatusStats = client().admin()
            .cluster()
            .prepareNodesStats()
            .execute()
            .actionGet()
            .getNodes()
            .get(0)
            .getIndices()
            .getIndexing()
            .getTotal()
            .getDocStatusStats();

        assertTrue(
            Arrays.equals(
                docStatusStats.getDocStatusCounter(),
                expectedDocStatusStats.getDocStatusCounter(),
                Comparator.comparingLong(AtomicLong::longValue)
            )
        );
    }

    private void updateExpectedDocStatusCounter(DocWriteResponse r) {
        expectedDocStatusStats.inc(r.status());
    }

    private void updateExpectedDocStatusCounter(Exception e) {
        expectedDocStatusStats.inc(ExceptionsHelper.status(e));
    }

    private enum MockStatsLevel {
        INDICES(NodeIndicesStats.StatsLevel.INDICES.getRestName()),
        SHARDS(NodeIndicesStats.StatsLevel.SHARDS.getRestName()),
        NODE(NodeIndicesStats.StatsLevel.NODE.getRestName()),
        NULL("null"),
        UNKNOWN("unknown");

        private final String restName;

        MockStatsLevel(String restName) {
            this.restName = restName;
        }

        public String getRestName() {
            return restName;
        }
    }

}
