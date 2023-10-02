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
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.shard.IndexingStats.Stats.DocStatusStats;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.hamcrest.MatcherAssert;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singletonMap;
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

}
