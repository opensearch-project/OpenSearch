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

package org.opensearch.action.bulk;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.action.support.replication.ReplicationMode;
import org.opensearch.action.support.replication.ReplicationTask;
import org.opensearch.action.support.replication.TransportReplicationAction.ReplicaResponse;
import org.opensearch.action.support.replication.TransportWriteAction.WritePrimaryResult;
import org.opensearch.action.update.UpdateHelper;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Requests;
import org.opensearch.cluster.action.index.MappingUpdatedAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.SegmentReplicationPressureService;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.Mapping;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.index.mapper.RootObjectMapper;
import org.opensearch.index.remote.RemoteRefreshSegmentPressureService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.SystemIndices;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;
import org.opensearch.transport.TestTransportChannel;
import org.opensearch.transport.TransportChannel;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportShardBulkActionTests extends IndexShardTestCase {

    private static final ActionListener<Void> ASSERTING_DONE_LISTENER = ActionTestUtils.assertNoFailureListener(r -> {});

    private final ShardId shardId = new ShardId("index", "_na_", 0);
    private final Settings idxSettings = Settings.builder()
        .put("index.number_of_shards", 1)
        .put("index.number_of_replicas", 0)
        .put("index.version.created", Version.CURRENT.id)
        .build();

    private IndexMetadata indexMetadata() throws IOException {
        return IndexMetadata.builder("index")
            .putMapping(
                "{\"properties\":{\"foo\":{\"type\":\"text\",\"fields\":" + "{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}"
            )
            .settings(idxSettings)
            .primaryTerm(0, 1)
            .build();
    }

    public void testExecuteBulkIndexRequest() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[1];
        boolean create = randomBoolean();
        DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE).create(create);
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);
        items[0] = primaryRequest;
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        assertFalse(context.hasMoreOperationsToExecute());

        // Translog should change, since there were no problems
        assertNotNull(context.getLocationToSync());

        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(create ? DocWriteRequest.OpType.CREATE : DocWriteRequest.OpType.INDEX));
        assertFalse(primaryResponse.isFailed());

        // Assert that the document actually made it there
        assertDocCount(shard, 1);

        writeRequest = new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE).create(true);
        primaryRequest = new BulkItemRequest(0, writeRequest);
        items[0] = primaryRequest;
        bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext secondContext = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            secondContext,
            null,
            threadPool::absoluteTimeInMillis,
            new ThrowingMappingUpdatePerformer(new RuntimeException("fail")),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        assertFalse(context.hasMoreOperationsToExecute());

        assertNull(secondContext.getLocationToSync());

        BulkItemRequest replicaRequest = bulkShardRequest.items()[0];

        primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.CREATE));
        // Should be failed since the document already exists
        assertTrue(primaryResponse.isFailed());

        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause().getClass(), equalTo(VersionConflictEngineException.class));
        assertThat(failure.getCause().getMessage(), containsString("version conflict, document already exists (current version [1])"));
        assertThat(failure.getStatus(), equalTo(RestStatus.CONFLICT));

        assertThat(replicaRequest, equalTo(primaryRequest));

        // Assert that the document count is still 1
        assertDocCount(shard, 1);
        closeShards(shard);
    }

    public void testSkipBulkIndexRequestIfAborted() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[randomIntBetween(2, 5)];
        for (int i = 0; i < items.length; i++) {
            DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index").id("id_" + i)
                .source(Requests.INDEX_CONTENT_TYPE)
                .opType(DocWriteRequest.OpType.INDEX);
            items[i] = new BulkItemRequest(i, writeRequest);
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        // Preemptively abort one of the bulk items, but allow the others to proceed
        BulkItemRequest rejectItem = randomFrom(items);
        RestStatus rejectionStatus = randomFrom(RestStatus.BAD_REQUEST, RestStatus.CONFLICT, RestStatus.FORBIDDEN, RestStatus.LOCKED);
        final OpenSearchStatusException rejectionCause = new OpenSearchStatusException("testing rejection", rejectionStatus);
        rejectItem.abort("index", rejectionCause);

        final CountDownLatch latch = new CountDownLatch(1);
        TransportShardBulkAction.performOnPrimary(
            bulkShardRequest,
            shard,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ActionListener.runAfter(ActionTestUtils.assertNoFailureListener(result -> {
                // since at least 1 item passed, the tran log location should exist,
                assertThat(((WritePrimaryResult<BulkShardRequest, BulkShardResponse>) result).location, notNullValue());
                // and the response should exist and match the item count
                assertThat(result.finalResponseIfSuccessful, notNullValue());
                assertThat(result.finalResponseIfSuccessful.getResponses(), arrayWithSize(items.length));

                // check each response matches the input item, including the rejection
                for (int i = 0; i < items.length; i++) {
                    BulkItemResponse response = result.finalResponseIfSuccessful.getResponses()[i];
                    assertThat(response.getItemId(), equalTo(i));
                    assertThat(response.getIndex(), equalTo("index"));
                    assertThat(response.getId(), equalTo("id_" + i));
                    assertThat(response.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
                    if (response.getItemId() == rejectItem.id()) {
                        assertTrue(response.isFailed());
                        assertThat(response.getFailure().getCause(), equalTo(rejectionCause));
                        assertThat(response.status(), equalTo(rejectionStatus));
                    } else {
                        assertFalse(response.isFailed());
                    }
                }

                // Check that the non-rejected updates made it to the shard
                try {
                    assertDocCount(shard, items.length - 1);
                    closeShards(shard);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }), latch::countDown),
            threadPool,
            Names.WRITE
        );

        latch.await();
    }

    public void testExecuteBulkIndexRequestWithMappingUpdates() throws Exception {

        BulkItemRequest[] items = new BulkItemRequest[1];
        DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        items[0] = new BulkItemRequest(0, writeRequest);
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        Engine.IndexResult mappingUpdate = new Engine.IndexResult(
            new Mapping(null, mock(RootObjectMapper.class), new MetadataFieldMapper[0], Collections.emptyMap())
        );
        Translog.Location resultLocation = new Translog.Location(42, 42, 42);
        Engine.IndexResult success = new FakeIndexResult(1, 1, 13, true, resultLocation);

        IndexShard shard = mock(IndexShard.class);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            mappingUpdate
        );
        when(shard.mapperService()).thenReturn(mock(MapperService.class));

        randomlySetIgnoredPrimaryResponse(items[0]);

        // Pretend the mappings haven't made it to the node yet
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        AtomicInteger updateCalled = new AtomicInteger();
        TransportShardBulkAction.executeBulkItemRequest(context, null, threadPool::absoluteTimeInMillis, (update, shardId, listener) -> {
            // There should indeed be a mapping update
            assertNotNull(update);
            updateCalled.incrementAndGet();
            listener.onResponse(null);
        }, listener -> listener.onResponse(null), ASSERTING_DONE_LISTENER);
        assertTrue(context.isInitial());
        assertTrue(context.hasMoreOperationsToExecute());

        assertThat("mappings were \"updated\" once", updateCalled.get(), equalTo(1));

        // Verify that the shard "executed" the operation once
        verify(shard, times(1)).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean());

        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            success
        );

        TransportShardBulkAction.executeBulkItemRequest(
            context,
            null,
            threadPool::absoluteTimeInMillis,
            (update, shardId, listener) -> fail("should not have had to update the mappings"),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );

        // Verify that the shard "executed" the operation only once (1 for previous invocations plus
        // 1 for this execution)
        verify(shard, times(2)).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean());

        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(writeRequest.opType()));
        assertFalse(primaryResponse.isFailed());

        closeShards(shard);
    }

    public void testExecuteBulkIndexRequestWithErrorWhileUpdatingMapping() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[1];
        DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
        items[0] = new BulkItemRequest(0, writeRequest);
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        // Return an exception when trying to update the mapping, or when waiting for it to come
        RuntimeException err = new RuntimeException("some kind of exception");

        boolean errorOnWait = randomBoolean();

        randomlySetIgnoredPrimaryResponse(items[0]);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        final CountDownLatch latch = new CountDownLatch(1);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            null,
            threadPool::absoluteTimeInMillis,
            errorOnWait == false ? new ThrowingMappingUpdatePerformer(err) : new NoopMappingUpdatePerformer(),
            errorOnWait ? listener -> listener.onFailure(err) : listener -> listener.onResponse(null),
            new LatchedActionListener<>(new ActionListener<Void>() {
                @Override
                public void onResponse(Void aVoid) {}

                @Override
                public void onFailure(final Exception e) {
                    assertEquals(err, e);
                }
            }, latch)
        );
        latch.await();
        assertFalse(context.hasMoreOperationsToExecute());

        // Translog shouldn't be synced, as there were conflicting mappings
        assertThat(context.getLocationToSync(), nullValue());

        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();

        // Since this was not a conflict failure, the primary response
        // should be filled out with the failure information
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
        assertTrue(primaryResponse.isFailed());
        assertThat(primaryResponse.getFailureMessage(), containsString("some kind of exception"));
        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause(), equalTo(err));

        closeShards(shard);
    }

    public void testExecuteBulkDeleteRequest() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[1];
        DocWriteRequest<DeleteRequest> writeRequest = new DeleteRequest("index", "id");
        items[0] = new BulkItemRequest(0, writeRequest);
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        Translog.Location location = new Translog.Location(0, 0, 0);

        randomlySetIgnoredPrimaryResponse(items[0]);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        assertFalse(context.hasMoreOperationsToExecute());

        // Translog changes, even though the document didn't exist
        assertThat(context.getLocationToSync(), not(location));

        BulkItemRequest replicaRequest = bulkShardRequest.items()[0];
        DocWriteRequest<?> replicaDeleteRequest = replicaRequest.request();
        BulkItemResponse primaryResponse = replicaRequest.getPrimaryResponse();
        DeleteResponse response = primaryResponse.getResponse();

        // Any version can be matched on replica
        assertThat(replicaDeleteRequest.version(), equalTo(Versions.MATCH_ANY));
        assertThat(replicaDeleteRequest.versionType(), equalTo(VersionType.INTERNAL));

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
        assertFalse(primaryResponse.isFailed());

        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.NOT_FOUND));
        assertThat(response.getShardId(), equalTo(shard.shardId()));
        assertThat(response.getIndex(), equalTo("index"));
        assertThat(response.getId(), equalTo("id"));
        assertThat(response.getVersion(), equalTo(1L));
        assertThat(response.getSeqNo(), equalTo(0L));
        assertThat(response.forcedRefresh(), equalTo(false));

        // Now do the same after indexing the document, it should now find and delete the document
        indexDoc(shard, "_doc", "id", "{}");

        writeRequest = new DeleteRequest("index", "id");
        items[0] = new BulkItemRequest(0, writeRequest);
        bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        location = context.getLocationToSync();

        randomlySetIgnoredPrimaryResponse(items[0]);

        context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        assertFalse(context.hasMoreOperationsToExecute());

        // Translog changes, because the document was deleted
        assertThat(context.getLocationToSync(), not(location));

        replicaRequest = bulkShardRequest.items()[0];
        replicaDeleteRequest = replicaRequest.request();
        primaryResponse = replicaRequest.getPrimaryResponse();
        response = primaryResponse.getResponse();

        // Any version can be matched on replica
        assertThat(replicaDeleteRequest.version(), equalTo(Versions.MATCH_ANY));
        assertThat(replicaDeleteRequest.versionType(), equalTo(VersionType.INTERNAL));

        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.DELETE));
        assertFalse(primaryResponse.isFailed());

        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.DELETED));
        assertThat(response.getShardId(), equalTo(shard.shardId()));
        assertThat(response.getIndex(), equalTo("index"));
        assertThat(response.getId(), equalTo("id"));
        assertThat(response.getVersion(), equalTo(3L));
        assertThat(response.getSeqNo(), equalTo(2L));
        assertThat(response.forcedRefresh(), equalTo(false));

        assertDocCount(shard, 0);
        closeShards(shard);
    }

    public void testNoopUpdateRequest() throws Exception {
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        DocWriteResponse noopUpdateResponse = new UpdateResponse(shardId, "id", 0, 2, 1, DocWriteResponse.Result.NOOP);

        IndexShard shard = mock(IndexShard.class);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(
                noopUpdateResponse,
                DocWriteResponse.Result.NOOP,
                Collections.singletonMap("field", "value"),
                Requests.INDEX_CONTENT_TYPE
            )
        );

        BulkItemRequest[] items = new BulkItemRequest[] { primaryRequest };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );

        assertFalse(context.hasMoreOperationsToExecute());

        // Basically nothing changes in the request since it's a noop
        assertThat(context.getLocationToSync(), nullValue());
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertThat(primaryResponse.getResponse(), equalTo(noopUpdateResponse));
        assertThat(primaryResponse.getResponse().getResult(), equalTo(DocWriteResponse.Result.NOOP));
        assertThat(bulkShardRequest.items().length, equalTo(1));
        assertEquals(primaryRequest, bulkShardRequest.items()[0]); // check that bulk item was not mutated
        assertThat(primaryResponse.getResponse().getSeqNo(), equalTo(0L));
    }

    public void testUpdateRequestWithFailure() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        IndexRequest updateResponse = new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value");

        Exception err = new OpenSearchException("I'm dead <(x.x)>");
        Engine.IndexResult indexResult = new Engine.IndexResult(err, 0, 0, 0);
        IndexShard shard = mock(IndexShard.class);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            indexResult
        );
        when(shard.indexSettings()).thenReturn(indexSettings);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(
                updateResponse,
                randomBoolean() ? DocWriteResponse.Result.CREATED : DocWriteResponse.Result.UPDATED,
                Collections.singletonMap("field", "value"),
                Requests.INDEX_CONTENT_TYPE
            )
        );

        BulkItemRequest[] items = new BulkItemRequest[] { primaryRequest };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        assertFalse(context.hasMoreOperationsToExecute());

        // Since this was not a conflict failure, the primary response
        // should be filled out with the failure information
        assertNull(context.getLocationToSync());
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertTrue(primaryResponse.isFailed());
        assertThat(primaryResponse.getFailureMessage(), containsString("I'm dead <(x.x)>"));
        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause(), equalTo(err));
        assertThat(failure.getStatus(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testUpdateRequestWithConflictFailure() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        IndexRequest updateResponse = new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value");

        Exception err = new VersionConflictEngineException(shardId, "id", "I'm conflicted <(;_;)>");
        Engine.IndexResult indexResult = new Engine.IndexResult(err, 0, 0, 0);
        IndexShard shard = mock(IndexShard.class);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            indexResult
        );
        when(shard.indexSettings()).thenReturn(indexSettings);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(
                updateResponse,
                randomBoolean() ? DocWriteResponse.Result.CREATED : DocWriteResponse.Result.UPDATED,
                Collections.singletonMap("field", "value"),
                Requests.INDEX_CONTENT_TYPE
            )
        );

        BulkItemRequest[] items = new BulkItemRequest[] { primaryRequest };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            ASSERTING_DONE_LISTENER
        );
        assertFalse(context.hasMoreOperationsToExecute());

        assertNull(context.getLocationToSync());
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertTrue(primaryResponse.isFailed());
        assertThat(primaryResponse.getFailureMessage(), containsString("I'm conflicted <(;_;)>"));
        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause(), equalTo(err));
        assertThat(failure.getStatus(), equalTo(RestStatus.CONFLICT));
    }

    public void testUpdateRequestWithSuccess() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        IndexRequest updateResponse = new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value");

        boolean created = randomBoolean();
        Translog.Location resultLocation = new Translog.Location(42, 42, 42);
        Engine.IndexResult indexResult = new FakeIndexResult(1, 1, 13, created, resultLocation);
        IndexShard shard = mock(IndexShard.class);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            indexResult
        );
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(
                updateResponse,
                created ? DocWriteResponse.Result.CREATED : DocWriteResponse.Result.UPDATED,
                Collections.singletonMap("field", "value"),
                Requests.INDEX_CONTENT_TYPE
            )
        );

        BulkItemRequest[] items = new BulkItemRequest[] { primaryRequest };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        assertFalse(context.hasMoreOperationsToExecute());

        // Check that the translog is successfully advanced
        assertThat(context.getLocationToSync(), equalTo(resultLocation));
        assertThat(bulkShardRequest.items()[0].request(), equalTo(updateResponse));
        // Since this was not a conflict failure, the primary response
        // should be filled out with the failure information
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        DocWriteResponse response = primaryResponse.getResponse();
        assertThat(response.status(), equalTo(created ? RestStatus.CREATED : RestStatus.OK));
        assertThat(response.getSeqNo(), equalTo(13L));
    }

    public void testUpdateWithDelete() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        DeleteRequest updateResponse = new DeleteRequest("index", "id");

        boolean found = randomBoolean();
        Translog.Location resultLocation = new Translog.Location(42, 42, 42);
        final long resultSeqNo = 13;
        Engine.DeleteResult deleteResult = new FakeDeleteResult(1, 1, resultSeqNo, found, resultLocation);
        IndexShard shard = mock(IndexShard.class);
        when(shard.applyDeleteOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong())).thenReturn(deleteResult);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(
                updateResponse,
                DocWriteResponse.Result.DELETED,
                Collections.singletonMap("field", "value"),
                Requests.INDEX_CONTENT_TYPE
            )
        );

        BulkItemRequest[] items = new BulkItemRequest[] { primaryRequest };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            ASSERTING_DONE_LISTENER
        );
        assertFalse(context.hasMoreOperationsToExecute());

        // Check that the translog is successfully advanced
        assertThat(context.getLocationToSync(), equalTo(resultLocation));
        assertThat(bulkShardRequest.items()[0].request(), equalTo(updateResponse));
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        DocWriteResponse response = primaryResponse.getResponse();
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertThat(response.getSeqNo(), equalTo(resultSeqNo));
    }

    public void testFailureDuringUpdateProcessing() throws Exception {
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        IndexShard shard = mock(IndexShard.class);

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        final OpenSearchException err = new OpenSearchException("oops");
        when(updateHelper.prepare(any(), eq(shard), any())).thenThrow(err);
        BulkItemRequest[] items = new BulkItemRequest[] { primaryRequest };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        assertFalse(context.hasMoreOperationsToExecute());

        assertNull(context.getLocationToSync());
        BulkItemResponse primaryResponse = bulkShardRequest.items()[0].getPrimaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertTrue(primaryResponse.isFailed());
        assertThat(primaryResponse.getFailureMessage(), containsString("oops"));
        BulkItemResponse.Failure failure = primaryResponse.getFailure();
        assertThat(failure.getIndex(), equalTo("index"));
        assertThat(failure.getId(), equalTo("id"));
        assertThat(failure.getCause(), equalTo(err));
        assertThat(failure.getStatus(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testTranslogPositionToSync() throws Exception {
        IndexShard shard = newStartedShard(true);

        BulkItemRequest[] items = new BulkItemRequest[randomIntBetween(2, 5)];
        for (int i = 0; i < items.length; i++) {
            DocWriteRequest<IndexRequest> writeRequest = new IndexRequest("index").id("id_" + i)
                .source(Requests.INDEX_CONTENT_TYPE)
                .opType(DocWriteRequest.OpType.INDEX);
            items[i] = new BulkItemRequest(i, writeRequest);
        }
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        while (context.hasMoreOperationsToExecute()) {
            TransportShardBulkAction.executeBulkItemRequest(
                context,
                null,
                threadPool::absoluteTimeInMillis,
                new NoopMappingUpdatePerformer(),
                listener -> {},
                ASSERTING_DONE_LISTENER
            );
        }

        assertTrue(shard.isSyncNeeded());

        // if we sync the location, nothing else is unsynced
        CountDownLatch latch = new CountDownLatch(1);
        shard.sync(context.getLocationToSync(), e -> {
            if (e != null) {
                throw new AssertionError(e);
            }
            latch.countDown();
        });

        latch.await();
        assertFalse(shard.isSyncNeeded());

        closeShards(shard);
    }

    public void testNoOpReplicationOnPrimaryDocumentFailure() throws Exception {
        final IndexShard shard = spy(newStartedShard(false));
        BulkItemRequest itemRequest = new BulkItemRequest(0, new IndexRequest("index").source(Requests.INDEX_CONTENT_TYPE));
        final String failureMessage = "simulated primary failure";
        final IOException exception = new IOException(failureMessage);
        itemRequest.setPrimaryResponse(
            new BulkItemResponse(
                0,
                randomFrom(DocWriteRequest.OpType.CREATE, DocWriteRequest.OpType.DELETE, DocWriteRequest.OpType.INDEX),
                new BulkItemResponse.Failure("index", "1", exception, 1L, 1L)
            )
        );
        BulkItemRequest[] itemRequests = new BulkItemRequest[1];
        itemRequests[0] = itemRequest;
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shard.shardId(), RefreshPolicy.NONE, itemRequests);
        TransportShardBulkAction.performOnReplica(bulkShardRequest, shard);
        verify(shard, times(1)).markSeqNoAsNoop(1, 1, exception.toString());
        closeShards(shard);
    }

    public void testRetries() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);
        UpdateRequest writeRequest = new UpdateRequest("index", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        // the beating will continue until success has come.
        writeRequest.retryOnConflict(Integer.MAX_VALUE);
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        IndexRequest updateResponse = new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value");

        Exception err = new VersionConflictEngineException(shardId, "id", "I'm conflicted <(;_;)>");
        Engine.IndexResult conflictedResult = new Engine.IndexResult(err, 0);
        Engine.IndexResult mappingUpdate = new Engine.IndexResult(
            new Mapping(null, mock(RootObjectMapper.class), new MetadataFieldMapper[0], Collections.emptyMap())
        );
        Translog.Location resultLocation = new Translog.Location(42, 42, 42);
        Engine.IndexResult success = new FakeIndexResult(1, 1, 13, true, resultLocation);

        IndexShard shard = mock(IndexShard.class);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenAnswer(ir -> {
            if (randomBoolean()) {
                return conflictedResult;
            }
            if (randomBoolean()) {
                return mappingUpdate;
            } else {
                return success;
            }
        });
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.mapperService()).thenReturn(mock(MapperService.class));

        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(
                updateResponse,
                randomBoolean() ? DocWriteResponse.Result.CREATED : DocWriteResponse.Result.UPDATED,
                Collections.singletonMap("field", "value"),
                Requests.INDEX_CONTENT_TYPE
            )
        );

        BulkItemRequest[] items = new BulkItemRequest[] { primaryRequest };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        final CountDownLatch latch = new CountDownLatch(1);
        TransportShardBulkAction.performOnPrimary(
            bulkShardRequest,
            shard,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
                assertThat(((WritePrimaryResult<BulkShardRequest, BulkShardResponse>) result).location, equalTo(resultLocation));
                BulkItemResponse primaryResponse = result.replicaRequest().items()[0].getPrimaryResponse();
                assertThat(primaryResponse.getItemId(), equalTo(0));
                assertThat(primaryResponse.getId(), equalTo("id"));
                assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
                DocWriteResponse response = primaryResponse.getResponse();
                assertThat(response.status(), equalTo(RestStatus.CREATED));
                assertThat(response.getSeqNo(), equalTo(13L));
            }), latch),
            threadPool,
            Names.WRITE
        );
        latch.await();
    }

    public void testForceExecutionOnRejectionAfterMappingUpdate() throws Exception {
        TestThreadPool rejectingThreadPool = new TestThreadPool(
            "TransportShardBulkActionTests#testForceExecutionOnRejectionAfterMappingUpdate",
            Settings.builder()
                .put("thread_pool." + ThreadPool.Names.WRITE + ".size", 1)
                .put("thread_pool." + ThreadPool.Names.WRITE + ".queue_size", 1)
                .build()
        );
        CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        rejectingThreadPool.executor(ThreadPool.Names.WRITE).execute(() -> {
            try {
                cyclicBarrier.await();
                logger.info("blocking the write executor");
                cyclicBarrier.await();
                logger.info("unblocked the write executor");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        try {
            cyclicBarrier.await();
            // Place a task in the queue to block next enqueue
            rejectingThreadPool.executor(ThreadPool.Names.WRITE).execute(() -> {});

            BulkItemRequest[] items = new BulkItemRequest[2];
            DocWriteRequest<IndexRequest> writeRequest1 = new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "foo", 1);
            DocWriteRequest<IndexRequest> writeRequest2 = new IndexRequest("index").id("id")
                .source(Requests.INDEX_CONTENT_TYPE, "foo", "bar");
            items[0] = new BulkItemRequest(0, writeRequest1);
            items[1] = new BulkItemRequest(1, writeRequest2);
            BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

            Engine.IndexResult mappingUpdate = new Engine.IndexResult(
                new Mapping(null, mock(RootObjectMapper.class), new MetadataFieldMapper[0], Collections.emptyMap())
            );
            Translog.Location resultLocation1 = new Translog.Location(42, 36, 36);
            Translog.Location resultLocation2 = new Translog.Location(42, 42, 42);
            Engine.IndexResult success1 = new FakeIndexResult(1, 1, 10, true, resultLocation1);
            Engine.IndexResult success2 = new FakeIndexResult(1, 1, 13, true, resultLocation2);

            IndexShard shard = mock(IndexShard.class);
            when(shard.shardId()).thenReturn(shardId);
            when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
                success1,
                mappingUpdate,
                success2
            );
            when(shard.getFailedIndexResult(any(OpenSearchRejectedExecutionException.class), anyLong())).thenCallRealMethod();
            when(shard.mapperService()).thenReturn(mock(MapperService.class));

            randomlySetIgnoredPrimaryResponse(items[0]);

            AtomicInteger updateCalled = new AtomicInteger();

            final CountDownLatch latch = new CountDownLatch(1);
            TransportShardBulkAction.performOnPrimary(
                bulkShardRequest,
                shard,
                null,
                rejectingThreadPool::absoluteTimeInMillis,
                (update, shardId, listener) -> {
                    // There should indeed be a mapping update
                    assertNotNull(update);
                    updateCalled.incrementAndGet();
                    listener.onResponse(null);
                    try {
                        // Release blocking task now that the continue write execution has been rejected and
                        // the finishRequest execution has been force enqueued
                        cyclicBarrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        throw new IllegalStateException(e);
                    }
                },
                listener -> listener.onResponse(null),
                new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result ->
                // Assert that we still need to fsync the location that was successfully written
                assertThat(((WritePrimaryResult<BulkShardRequest, BulkShardResponse>) result).location, equalTo(resultLocation1))), latch),
                rejectingThreadPool,
                Names.WRITE
            );
            latch.await();

            assertThat("mappings were \"updated\" once", updateCalled.get(), equalTo(1));

            verify(shard, times(2)).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean());

            BulkItemResponse primaryResponse1 = bulkShardRequest.items()[0].getPrimaryResponse();
            assertThat(primaryResponse1.getItemId(), equalTo(0));
            assertThat(primaryResponse1.getId(), equalTo("id"));
            assertThat(primaryResponse1.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
            assertFalse(primaryResponse1.isFailed());
            assertThat(primaryResponse1.getResponse().status(), equalTo(RestStatus.CREATED));
            assertThat(primaryResponse1.getResponse().getSeqNo(), equalTo(10L));

            BulkItemResponse primaryResponse2 = bulkShardRequest.items()[1].getPrimaryResponse();
            assertThat(primaryResponse2.getItemId(), equalTo(1));
            assertThat(primaryResponse2.getId(), equalTo("id"));
            assertThat(primaryResponse2.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
            assertTrue(primaryResponse2.isFailed());
            assertNull(primaryResponse2.getResponse());
            assertEquals(primaryResponse2.status(), RestStatus.TOO_MANY_REQUESTS);
            assertThat(primaryResponse2.getFailure().getCause(), instanceOf(OpenSearchRejectedExecutionException.class));

            closeShards(shard);
        } finally {
            rejectingThreadPool.shutdownNow();
        }
    }

    public void testHandlePrimaryTermValidationRequestWithDifferentAllocationId() {

        final String aId = "test-allocation-id";
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ReplicationTask task = createReplicationTask();
        PlainActionFuture<TransportResponse> listener = new PlainActionFuture<>();
        TransportShardBulkAction action = new TransportShardBulkAction(
            Settings.EMPTY,
            mock(TransportService.class),
            mockClusterService(),
            mockIndicesService(aId, 1L),
            threadPool,
            mock(ShardStateAction.class),
            mock(MappingUpdatedAction.class),
            mock(UpdateHelper.class),
            mock(ActionFilters.class),
            mock(IndexingPressureService.class),
            mock(SegmentReplicationPressureService.class),
            mock(RemoteRefreshSegmentPressureService.class),
            mock(SystemIndices.class)
        );
        action.handlePrimaryTermValidationRequest(
            new TransportShardBulkAction.PrimaryTermValidationRequest(aId + "-1", 1, shardId),
            createTransportChannel(listener),
            task
        );
        assertThrows(ShardNotFoundException.class, listener::actionGet);
        assertNotNull(task.getPhase());
        assertEquals("failed", task.getPhase());
    }

    public void testHandlePrimaryTermValidationRequestWithOlderPrimaryTerm() {

        final String aId = "test-allocation-id";
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ReplicationTask task = createReplicationTask();
        PlainActionFuture<TransportResponse> listener = new PlainActionFuture<>();
        TransportShardBulkAction action = new TransportShardBulkAction(
            Settings.EMPTY,
            mock(TransportService.class),
            mockClusterService(),
            mockIndicesService(aId, 2L),
            threadPool,
            mock(ShardStateAction.class),
            mock(MappingUpdatedAction.class),
            mock(UpdateHelper.class),
            mock(ActionFilters.class),
            mock(IndexingPressureService.class),
            mock(SegmentReplicationPressureService.class),
            mock(RemoteRefreshSegmentPressureService.class),
            mock(SystemIndices.class)
        );
        action.handlePrimaryTermValidationRequest(
            new TransportShardBulkAction.PrimaryTermValidationRequest(aId, 1, shardId),
            createTransportChannel(listener),
            task
        );
        assertThrows(IllegalStateException.class, listener::actionGet);
        assertNotNull(task.getPhase());
        assertEquals("failed", task.getPhase());
    }

    public void testHandlePrimaryTermValidationRequestSuccess() {

        final String aId = "test-allocation-id";
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ReplicationTask task = createReplicationTask();
        PlainActionFuture<TransportResponse> listener = new PlainActionFuture<>();
        TransportShardBulkAction action = new TransportShardBulkAction(
            Settings.EMPTY,
            mock(TransportService.class),
            mockClusterService(),
            mockIndicesService(aId, 1L),
            threadPool,
            mock(ShardStateAction.class),
            mock(MappingUpdatedAction.class),
            mock(UpdateHelper.class),
            mock(ActionFilters.class),
            mock(IndexingPressureService.class),
            mock(SegmentReplicationPressureService.class),
            mock(RemoteRefreshSegmentPressureService.class),
            mock(SystemIndices.class)
        );
        action.handlePrimaryTermValidationRequest(
            new TransportShardBulkAction.PrimaryTermValidationRequest(aId, 1, shardId),
            createTransportChannel(listener),
            task
        );
        assertTrue(listener.actionGet() instanceof ReplicaResponse);
        assertEquals(SequenceNumbers.NO_OPS_PERFORMED, ((ReplicaResponse) listener.actionGet()).localCheckpoint());
        assertEquals(SequenceNumbers.NO_OPS_PERFORMED, ((ReplicaResponse) listener.actionGet()).globalCheckpoint());
        assertNotNull(task.getPhase());
        assertEquals("finished", task.getPhase());
    }

    public void testGetReplicationModeWithRemoteTranslog() {
        TransportShardBulkAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.isRemoteTranslogEnabled()).thenReturn(true);
        assertEquals(ReplicationMode.PRIMARY_TERM_VALIDATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeWithLocalTranslog() {
        TransportShardBulkAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.isRemoteTranslogEnabled()).thenReturn(false);
        assertEquals(ReplicationMode.FULL_REPLICATION, action.getReplicationMode(indexShard));
    }

    private TransportShardBulkAction createAction() {
        return new TransportShardBulkAction(
            Settings.EMPTY,
            mock(TransportService.class),
            mockClusterService(),
            mock(IndicesService.class),
            threadPool,
            mock(ShardStateAction.class),
            mock(MappingUpdatedAction.class),
            mock(UpdateHelper.class),
            mock(ActionFilters.class),
            mock(IndexingPressureService.class),
            mock(SegmentReplicationPressureService.class),
            mock(RemoteRefreshSegmentPressureService.class),
            mock(SystemIndices.class)
        );
    }

    private ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }

    private IndicesService mockIndicesService(String aId, long primaryTerm) {
        // Mock few of the required classes
        IndicesService indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(indicesService.indexServiceSafe(any(Index.class))).thenReturn(indexService);
        when(indexService.getShard(anyInt())).thenReturn(indexShard);
        when(indexShard.getOperationPrimaryTerm()).thenReturn(primaryTerm);

        // Mock routing entry, allocation id
        AllocationId allocationId = mock(AllocationId.class);
        ShardRouting shardRouting = mock(ShardRouting.class);
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(shardRouting.allocationId()).thenReturn(allocationId);
        when(allocationId.getId()).thenReturn(aId);
        return indicesService;
    }

    private ReplicationTask createReplicationTask() {
        return new ReplicationTask(0, null, null, null, null, null);
    }

    /**
     * Transport channel that is needed for replica operation testing.
     */
    private TransportChannel createTransportChannel(final PlainActionFuture<TransportResponse> listener) {
        return new TestTransportChannel(listener);
    }

    private void randomlySetIgnoredPrimaryResponse(BulkItemRequest primaryRequest) {
        if (randomBoolean()) {
            // add a response to the request and thereby check that it is ignored for the primary.
            primaryRequest.setPrimaryResponse(
                new BulkItemResponse(
                    0,
                    DocWriteRequest.OpType.INDEX,
                    new IndexResponse(shardId, "ignore-primary-response-on-primary", 42, 42, 42, false)
                )
            );
        }
    }

    /**
     * Fake IndexResult that has a settable translog location
     */
    static class FakeIndexResult extends Engine.IndexResult {

        private final Translog.Location location;

        protected FakeIndexResult(long version, long term, long seqNo, boolean created, Translog.Location location) {
            super(version, term, seqNo, created);
            this.location = location;
        }

        @Override
        public Translog.Location getTranslogLocation() {
            return this.location;
        }
    }

    /**
     * Fake DeleteResult that has a settable translog location
     */
    static class FakeDeleteResult extends Engine.DeleteResult {

        private final Translog.Location location;

        protected FakeDeleteResult(long version, long term, long seqNo, boolean found, Translog.Location location) {
            super(version, term, seqNo, found);
            this.location = location;
        }

        @Override
        public Translog.Location getTranslogLocation() {
            return this.location;
        }
    }

    /** Doesn't perform any mapping updates */
    public static class NoopMappingUpdatePerformer implements MappingUpdatePerformer {
        @Override
        public void updateMappings(Mapping update, ShardId shardId, ActionListener<Void> listener) {
            listener.onResponse(null);
        }
    }

    /** Always throw the given exception */
    private class ThrowingMappingUpdatePerformer implements MappingUpdatePerformer {
        private final RuntimeException e;

        ThrowingMappingUpdatePerformer(RuntimeException e) {
            this.e = e;
        }

        @Override
        public void updateMappings(Mapping update, ShardId shardId, ActionListener<Void> listener) {
            listener.onFailure(e);
        }
    }
}
