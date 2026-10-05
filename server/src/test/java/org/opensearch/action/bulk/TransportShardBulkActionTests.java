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

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
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
import org.opensearch.action.support.replication.TransportReplicationAction.PrimaryResult;
import org.opensearch.action.support.replication.TransportReplicationAction.ReplicaResponse;
import org.opensearch.action.support.replication.TransportWriteAction.WritePrimaryResult;
import org.opensearch.action.update.UpdateHelper;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.action.index.MappingUpdatedAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.SegmentReplicationPressureService;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.Mapping;
import org.opensearch.index.mapper.MetadataFieldMapper;
import org.opensearch.index.mapper.RootObjectMapper;
import org.opensearch.index.remote.RemoteStorePressureService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.SystemIndices;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPool.Names;
import org.opensearch.transport.TestTransportChannel;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Requests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import org.mockito.InOrder;

import static org.opensearch.index.remote.RemoteStoreTestsHelper.createIndexSettings;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
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

        items[0] = randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        BulkShardRequest completedRequest = context.getBulkShardRequest();
        assertFalse(context.hasMoreOperationsToExecute());

        // Translog should change, since there were no problems
        assertNotNull(context.getLocationToSync());

        BulkItemResponse primaryResponse = completedRequest.items()[0].primaryResponse();

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

        items[0] = randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext secondContext = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            secondContext,
            null,
            threadPool::absoluteTimeInMillis,
            new ThrowingMappingUpdatePerformer(new RuntimeException("fail")),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        completedRequest = secondContext.getBulkShardRequest();
        assertFalse(context.hasMoreOperationsToExecute());

        assertNull(secondContext.getLocationToSync());

        BulkItemRequest replicaRequest = completedRequest.items()[0];

        primaryResponse = replicaRequest.primaryResponse();

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

        assertEquals(primaryRequest.request(), replicaRequest.request());
        assertEquals(primaryRequest.index(), replicaRequest.index());
        assertEquals(primaryRequest.id(), replicaRequest.id());

        // Assert that the document count is still 1
        assertDocCount(shard, 1);
        closeShards(shard);
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

        items[0] = randomlySetIgnoredPrimaryResponse(items[0]);

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
        BulkShardRequest completedRequest = context.getBulkShardRequest();

        // Verify that the shard "executed" the operation only once (1 for previous invocations plus
        // 1 for this execution)
        verify(shard, times(2)).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean());

        BulkItemResponse primaryResponse = completedRequest.items()[0].primaryResponse();

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

        items[0] = randomlySetIgnoredPrimaryResponse(items[0]);

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
        BulkShardRequest completedRequest = context.getBulkShardRequest();

        // Translog shouldn't be synced, as there were conflicting mappings
        assertThat(context.getLocationToSync(), nullValue());

        BulkItemResponse primaryResponse = completedRequest.items()[0].primaryResponse();

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

        items[0] = randomlySetIgnoredPrimaryResponse(items[0]);

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
        BulkShardRequest completedRequest = context.getBulkShardRequest();

        // Translog changes, even though the document didn't exist
        assertThat(context.getLocationToSync(), not(location));

        BulkItemRequest replicaRequest = completedRequest.items()[0];
        DocWriteRequest<?> replicaDeleteRequest = replicaRequest.request();
        BulkItemResponse primaryResponse = replicaRequest.primaryResponse();
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

        items[0] = randomlySetIgnoredPrimaryResponse(items[0]);

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
        completedRequest = context.getBulkShardRequest();

        // Translog changes, because the document was deleted
        assertThat(context.getLocationToSync(), not(location));

        replicaRequest = completedRequest.items()[0];
        replicaDeleteRequest = replicaRequest.request();
        primaryResponse = replicaRequest.primaryResponse();
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

        items[0] = randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        BulkShardRequest completedRequest = context.getBulkShardRequest();

        assertFalse(context.hasMoreOperationsToExecute());

        // Basically nothing changes in the request since it's a noop
        assertThat(context.getLocationToSync(), nullValue());
        BulkItemResponse primaryResponse = completedRequest.items()[0].primaryResponse();
        assertThat(primaryResponse.getItemId(), equalTo(0));
        assertThat(primaryResponse.getId(), equalTo("id"));
        assertThat(primaryResponse.getOpType(), equalTo(DocWriteRequest.OpType.UPDATE));
        assertThat(primaryResponse.getResponse(), equalTo(noopUpdateResponse));
        assertThat(primaryResponse.getResponse().getResult(), equalTo(DocWriteResponse.Result.NOOP));
        assertThat(completedRequest.items().length, equalTo(1));
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

        items[0] = randomlySetIgnoredPrimaryResponse(primaryRequest);

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
        BulkShardRequest completedRequest = context.getBulkShardRequest();
        BulkItemResponse primaryResponse = completedRequest.items()[0].primaryResponse();
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

        items[0] = randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            ASSERTING_DONE_LISTENER
        );
        BulkShardRequest completedRequest = context.getBulkShardRequest();
        assertFalse(context.hasMoreOperationsToExecute());

        assertNull(context.getLocationToSync());
        BulkItemResponse primaryResponse = completedRequest.items()[0].primaryResponse();
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

        items[0] = randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        BulkShardRequest completedRequest = context.getBulkShardRequest();
        assertFalse(context.hasMoreOperationsToExecute());

        // Check that the translog is successfully advanced
        assertThat(context.getLocationToSync(), equalTo(resultLocation));
        assertThat(completedRequest.items()[0].request(), equalTo(updateResponse));
        // Since this was not a conflict failure, the primary response
        // should be filled out with the failure information
        BulkItemResponse primaryResponse = completedRequest.items()[0].primaryResponse();
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
        when(shard.applyDeleteOperationOnPrimary(anyLong(), any(), any(), any(), anyLong(), anyLong())).thenReturn(deleteResult);
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

        items[0] = randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            ASSERTING_DONE_LISTENER
        );
        BulkShardRequest completedRequest = context.getBulkShardRequest();
        assertFalse(context.hasMoreOperationsToExecute());

        // Check that the translog is successfully advanced
        assertThat(context.getLocationToSync(), equalTo(resultLocation));
        assertThat(completedRequest.items()[0].request(), equalTo(updateResponse));
        BulkItemResponse primaryResponse = completedRequest.items()[0].primaryResponse();
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

        items[0] = randomlySetIgnoredPrimaryResponse(primaryRequest);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(bulkShardRequest, shard);
        TransportShardBulkAction.executeBulkItemRequest(
            context,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> {},
            ASSERTING_DONE_LISTENER
        );
        BulkShardRequest completedRequest = context.getBulkShardRequest();
        assertFalse(context.hasMoreOperationsToExecute());

        assertNull(context.getLocationToSync());
        BulkItemResponse primaryResponse = completedRequest.items()[0].primaryResponse();
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

    public void testBatchedTranslogFinishFailureFailsRequestAcknowledgement() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);
        BulkItemRequest item = new BulkItemRequest(
            0,
            new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value")
        );
        BulkShardRequest request = new BulkShardRequest(shardId, RefreshPolicy.NONE, new BulkItemRequest[] { item });

        IndexShard shard = mock(IndexShard.class);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            new FakeIndexResult(1, 1, 0, true, null)
        );
        Engine.TranslogBatch batch = mock(Engine.TranslogBatch.class);
        EngineException appendFailure = new EngineException(shardId, "simulated batched append failure");
        when(batch.finish()).thenThrow(appendFailure);
        when(shard.beginTranslogBatch()).thenReturn(batch);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> observedFailure = new AtomicReference<>();
        TransportShardBulkAction.performOnPrimary(
            request,
            shard,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(new ActionListener<>() {
                @Override
                public void onResponse(PrimaryResult<BulkShardRequest, BulkShardResponse> response) {
                    fail("request must not be acknowledged after a batched translog append failure");
                }

                @Override
                public void onFailure(Exception e) {
                    observedFailure.set(e);
                }
            }, latch),
            threadPool,
            Names.WRITE
        );

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertThat(observedFailure.get(), equalTo(appendFailure));
    }

    /**
     * A batched append that fails while the runnable is yielding for a mapping update must fail the request exactly
     * once. The mapping-update continuation re-executes the runnable; without deferring, the yield-time finish failure
     * would fail the request and the re-execution would then complete it a second time.
     */
    public void testBatchFinishFailureDuringMappingUpdateYieldFailsRequestExactlyOnce() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "foo", "bar")) };
        BulkShardRequest request = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        IndexShard shard = mock(IndexShard.class);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.mapperService()).thenReturn(mock(MapperService.class));
        Engine.IndexResult mappingUpdate = new Engine.IndexResult(
            new Mapping(null, mock(RootObjectMapper.class), new MetadataFieldMapper[0], Collections.emptyMap())
        );
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            mappingUpdate
        );
        Engine.TranslogBatch batch = mock(Engine.TranslogBatch.class);
        TranslogException appendFailure = new TranslogException(shardId, "simulated append failure at yield");
        when(batch.finish()).thenThrow(appendFailure);
        when(shard.beginTranslogBatch()).thenReturn(batch);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger completions = new AtomicInteger();
        AtomicReference<Exception> observedFailure = new AtomicReference<>();
        TransportShardBulkAction.performOnPrimary(
            request,
            shard,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            // resume synchronously via the listener, as the real cluster-state observer would on another thread
            listener -> listener.onResponse(null),
            new ActionListener<>() {
                @Override
                public void onResponse(PrimaryResult<BulkShardRequest, BulkShardResponse> response) {
                    completions.incrementAndGet();
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    completions.incrementAndGet();
                    observedFailure.set(e);
                    latch.countDown();
                }
            },
            threadPool,
            Names.WRITE
        );

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertBusy(() -> assertThat(completions.get(), equalTo(1)));
        assertThat(observedFailure.get(), sameInstance(appendFailure));
        // the first scope was finished (and failed) at the yield; the re-execution surfaced the failure before opening another
        verify(shard, times(1)).beginTranslogBatch();
        verify(batch, times(1)).finish();
        verify(shard, times(1)).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean());
    }

    /**
     * When the request body throws (as a per-operation translog failure does today) and the final-chunk append then
     * fails too, the body's exception is what the client sees, with the finish failure attached as suppressed. The
     * finally-block finish must not replace it.
     */
    public void testBatchFinishFailureDoesNotMaskBodyFailure() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);
        BulkItemRequest item = new BulkItemRequest(
            0,
            new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value")
        );
        BulkShardRequest request = new BulkShardRequest(shardId, RefreshPolicy.NONE, new BulkItemRequest[] { item });

        IndexShard shard = mock(IndexShard.class);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);
        TranslogException bodyFailure = new TranslogException(shardId, "simulated inline translog failure");
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenThrow(
            bodyFailure
        );
        Engine.TranslogBatch batch = mock(Engine.TranslogBatch.class);
        AlreadyClosedException finishFailure = new AlreadyClosedException("translog is already closed");
        when(batch.finish()).thenThrow(finishFailure);
        when(shard.beginTranslogBatch()).thenReturn(batch);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> observedFailure = new AtomicReference<>();
        TransportShardBulkAction.performOnPrimary(
            request,
            shard,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(new ActionListener<>() {
                @Override
                public void onResponse(PrimaryResult<BulkShardRequest, BulkShardResponse> response) {
                    fail("request must not be acknowledged");
                }

                @Override
                public void onFailure(Exception e) {
                    observedFailure.set(e);
                }
            }, latch),
            threadPool,
            Names.WRITE
        );

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertThat(observedFailure.get(), sameInstance(bodyFailure));
        assertThat(Arrays.asList(bodyFailure.getSuppressed()), contains(sameInstance(finishFailure)));
        verify(batch, times(1)).finish();
    }

    /**
     * The pending index chunk must be appended (flushed) before an UPDATE and before a DELETE are applied, so that an
     * update/delete can never be ordered ahead of an index operation it logically follows. This asserts the exact
     * interleaving of {@link Engine.TranslogBatch#flush()} with the shard apply calls using a single {@link InOrder}
     * over both the batch and the shard, for a mixed INDEX, UPDATE, DELETE bulk request.
     */
    public void testPendingIndexBatchFlushesBeforeUpdateAndDeleteBoundaries() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);

        // Mixed request: an index that fills the batch, then an update, then a delete. Each of the update and delete
        // must be preceded by a flush of the pending index chunk.
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, new IndexRequest("index").id("idx").source(Requests.INDEX_CONTENT_TYPE, "field", "value")),
            new BulkItemRequest(1, new UpdateRequest("index", "upd").doc(Requests.INDEX_CONTENT_TYPE, "field", "value")),
            new BulkItemRequest(2, new DeleteRequest("index", "del")) };
        BulkShardRequest request = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        IndexShard shard = mock(IndexShard.class);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);

        Engine.TranslogBatch batch = mock(Engine.TranslogBatch.class);
        when(shard.beginTranslogBatch()).thenReturn(batch);

        // Index op succeeds and leaves a chunk pending in the batch.
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            new FakeIndexResult(1, 1, 10, true, new Translog.Location(10, 10, 10)),
            new FakeIndexResult(1, 1, 11, true, new Translog.Location(11, 11, 11))
        );
        // Update is applied as an index op after UpdateHelper.prepare; distinguish it from the delete via the result.
        UpdateHelper updateHelper = mock(UpdateHelper.class);
        IndexRequest updatedDoc = new IndexRequest("index").id("upd").source(Requests.INDEX_CONTENT_TYPE, "field", "value");
        when(updateHelper.prepare(any(), eq(shard), any())).thenReturn(
            new UpdateHelper.Result(
                updatedDoc,
                DocWriteResponse.Result.UPDATED,
                Collections.singletonMap("field", "value"),
                Requests.INDEX_CONTENT_TYPE
            )
        );
        when(shard.applyDeleteOperationOnPrimary(anyLong(), any(), any(), any(), anyLong(), anyLong())).thenReturn(
            new FakeDeleteResult(1, 1, 12, true, new Translog.Location(12, 12, 12))
        );

        CountDownLatch latch = new CountDownLatch(1);
        TransportShardBulkAction.performOnPrimary(
            request,
            shard,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {}), latch),
            threadPool,
            Names.WRITE
        );
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        // A single ordered verification across both mocks proves the interleaving:
        // begin -> apply index -> flush (before update) -> apply update -> flush (before delete) -> apply delete -> finish
        InOrder inOrder = inOrder(shard, batch);
        inOrder.verify(shard).beginTranslogBatch();
        inOrder.verify(shard).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean());
        inOrder.verify(batch).flush();
        // the update is executed as an index operation on the shard
        inOrder.verify(shard).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean());
        inOrder.verify(batch).flush();
        inOrder.verify(shard).applyDeleteOperationOnPrimary(anyLong(), any(), any(), any(), anyLong(), anyLong());
        inOrder.verify(batch).finish();
    }

    /**
     * A batched index result carries no translog location when it is recorded; its chunk is appended by the flush that
     * precedes the delete. The delete is then written inline at a later location, and {@code finish()} reports only
     * the greatest location the batch itself appended, which is older than the delete. The request must sync to the
     * delete's location, otherwise an acknowledged delete can escape the sync (or the remote upload after a generation
     * roll). Regression test for the review finding on PR #23224.
     */
    public void testInlineDeleteAfterLastBatchFlushWinsOverOlderFinishLocation() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);

        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, new IndexRequest("index").id("idx").source(Requests.INDEX_CONTENT_TYPE, "field", "value")),
            new BulkItemRequest(1, new DeleteRequest("index", "del")) };
        BulkShardRequest request = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        IndexShard shard = mock(IndexShard.class);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);

        final Translog.Location chunkLocation = new Translog.Location(1, 10, 10);
        final Translog.Location deleteLocation = new Translog.Location(1, 20, 10);
        Engine.TranslogBatch batch = mock(Engine.TranslogBatch.class);
        when(shard.beginTranslogBatch()).thenReturn(batch);
        // The flush before the delete appends the pending index chunk; finish has nothing left and reports the same
        // (older) greatest batch location.
        when(batch.flush()).thenReturn(chunkLocation);
        when(batch.finish()).thenReturn(chunkLocation);

        // Deferred index result: no location yet, as the real engines return under batching.
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            new FakeIndexResult(1, 1, 10, true, null)
        );
        when(shard.applyDeleteOperationOnPrimary(anyLong(), any(), any(), any(), anyLong(), anyLong())).thenReturn(
            new FakeDeleteResult(1, 1, 11, true, deleteLocation)
        );

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Translog.Location> synced = new AtomicReference<>();
        TransportShardBulkAction.performOnPrimary(
            request,
            shard,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(
                ActionTestUtils.assertNoFailureListener(
                    result -> synced.set(((WritePrimaryResult<BulkShardRequest, BulkShardResponse>) result).location)
                ),
                latch
            ),
            threadPool,
            Names.WRITE
        );
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        InOrder inOrder = inOrder(shard, batch);
        inOrder.verify(shard).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean());
        inOrder.verify(batch).flush();
        inOrder.verify(shard).applyDeleteOperationOnPrimary(anyLong(), any(), any(), any(), anyLong(), anyLong());
        inOrder.verify(batch).finish();
        assertThat(synced.get(), equalTo(deleteLocation));
    }

    /**
     * Dynamic mapping update in the middle of a batched bulk: the first document is deferred (null location) into
     * scope 1, the second needs a mapping update, so scope 1 is finished before the yield and reports its chunk
     * location; the retry runs on another thread in scope 2, whose finish reports a later location. The request must
     * sync to the later location, and both items must be acknowledged.
     */
    public void testMappingUpdateMidBatchSyncsToLaterScopeLocation() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);

        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, new IndexRequest("index").id("a").source(Requests.INDEX_CONTENT_TYPE, "foo", "bar")),
            new BulkItemRequest(1, new IndexRequest("index").id("b").source(Requests.INDEX_CONTENT_TYPE, "newfield", "baz")) };
        BulkShardRequest request = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        IndexShard shard = mock(IndexShard.class);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.mapperService()).thenReturn(mock(MapperService.class));

        Engine.IndexResult deferredA = new FakeIndexResult(1, 1, 0, true, null);
        Engine.IndexResult mappingUpdate = new Engine.IndexResult(
            new Mapping(null, mock(RootObjectMapper.class), new MetadataFieldMapper[0], Collections.emptyMap())
        );
        Engine.IndexResult deferredB = new FakeIndexResult(1, 1, 1, true, null);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            deferredA,
            mappingUpdate,
            deferredB
        );

        final Translog.Location scope1Location = new Translog.Location(1, 10, 10);
        final Translog.Location scope2Location = new Translog.Location(1, 30, 10);
        List<Engine.TranslogBatch> batches = new CopyOnWriteArrayList<>();
        when(shard.beginTranslogBatch()).thenAnswer(invocation -> {
            Engine.TranslogBatch batch = mock(Engine.TranslogBatch.class);
            Translog.Location loc = batches.isEmpty() ? scope1Location : scope2Location;
            when(batch.flush()).thenReturn(loc);
            when(batch.finish()).thenReturn(loc);
            batches.add(batch);
            return batch;
        });

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<WritePrimaryResult<BulkShardRequest, BulkShardResponse>> primaryResult = new AtomicReference<>();
        TransportShardBulkAction.performOnPrimary(
            request,
            shard,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(
                ActionTestUtils.assertNoFailureListener(
                    result -> primaryResult.set((WritePrimaryResult<BulkShardRequest, BulkShardResponse>) result)
                ),
                latch
            ),
            threadPool,
            Names.WRITE
        );
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertThat(batches.size(), equalTo(2));
        verify(batches.get(0)).finish();
        verify(batches.get(1)).finish();
        assertThat(primaryResult.get().location, equalTo(scope2Location));
        for (BulkItemRequest item : primaryResult.get().replicaRequest().items()) {
            assertNotNull(item.primaryResponse());
            assertFalse(item.primaryResponse().isFailed());
        }
    }

    /**
     * When an operation requires a dynamic mapping update, execution yields the thread. Before yielding, the current
     * batch scope must be finalized with {@link Engine.TranslogBatch#finish()} (so a pending chunk never crosses
     * threads), and when execution resumes on another thread it must open a brand-new batch via
     * {@link IndexShard#beginTranslogBatch()}. This verifies two distinct begin calls on two distinct threads and a
     * finish for each scope, with no sleeps - the resume is driven by the mapping-update listener callbacks.
     */
    public void testMappingUpdateYieldFinishesScopeAndResumedExecutionBeginsFreshBatch() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);

        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "foo", "bar")) };
        BulkShardRequest request = new BulkShardRequest(shardId, RefreshPolicy.NONE, items);

        IndexShard shard = mock(IndexShard.class);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.mapperService()).thenReturn(mock(MapperService.class));

        // First attempt requires a mapping update (yield), the retry after the update succeeds.
        Engine.IndexResult mappingUpdate = new Engine.IndexResult(
            new Mapping(null, mock(RootObjectMapper.class), new MetadataFieldMapper[0], Collections.emptyMap())
        );
        Engine.IndexResult success = new FakeIndexResult(1, 1, 10, true, new Translog.Location(10, 10, 10));
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            mappingUpdate,
            success
        );

        // Each doRun() invocation opens a fresh batch; hand out a distinct mock per begin call and record the calling
        // thread so we can prove the resumed execution ran on a different thread.
        List<Engine.TranslogBatch> batches = new CopyOnWriteArrayList<>();
        List<String> beginThreads = new CopyOnWriteArrayList<>();
        when(shard.beginTranslogBatch()).thenAnswer(invocation -> {
            Engine.TranslogBatch batch = mock(Engine.TranslogBatch.class);
            batches.add(batch);
            beginThreads.add(Thread.currentThread().getName());
            return batch;
        });

        CountDownLatch latch = new CountDownLatch(1);
        TransportShardBulkAction.performOnPrimary(
            request,
            shard,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            // resume synchronously via the listener - no sleeps, deterministic
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {}), latch),
            threadPool,
            Names.WRITE
        );
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        // Exactly two batch scopes: the original (yielded) scope and the resumed scope.
        assertThat(batches.size(), equalTo(2));
        assertThat(beginThreads.size(), equalTo(2));

        // Both operations were attempted (mapping-update attempt + successful retry).
        verify(shard, times(2)).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean());

        // The first scope was finalized before the yield, and the resumed scope was finalized at completion.
        verify(batches.get(0)).finish();
        verify(batches.get(1)).finish();

        // The resumed execution began its fresh batch on a different thread than the original invocation, proving the
        // scope was detached and did not cross threads.
        assertThat(beginThreads.get(1), not(equalTo(beginThreads.get(0))));
    }

    public void testFailedUpdatePreparationDoesNotTriggerRefresh() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);

        // Create an update request that will fail during preparation
        DocWriteRequest<UpdateRequest> writeRequest = new UpdateRequest("index", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value");
        BulkItemRequest primaryRequest = new BulkItemRequest(0, writeRequest);

        IndexShard shard = mock(IndexShard.class);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);

        // Mock the UpdateHelper to throw a version conflict exception during preparation
        UpdateHelper updateHelper = mock(UpdateHelper.class);
        final VersionConflictEngineException versionConflict = new VersionConflictEngineException(
            shardId,
            "id",
            "version conflict during update preparation"
        );
        when(updateHelper.prepare(any(), eq(shard), any())).thenThrow(versionConflict);

        // Create bulk request with IMMEDIATE refresh policy
        BulkItemRequest[] items = new BulkItemRequest[] { primaryRequest };
        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.IMMEDIATE, items);

        items[0] = randomlySetIgnoredPrimaryResponse(primaryRequest);

        // Execute the bulk operation through performOnPrimary
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean refreshCalled = new AtomicBoolean(false);

        // Mock refresh to track if it's called
        doAnswer(invocation -> {
            refreshCalled.set(true);
            return null;
        }).when(shard).refresh(any());

        TransportShardBulkAction.performOnPrimary(
            bulkShardRequest,
            shard,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
                WritePrimaryResult<BulkShardRequest, BulkShardResponse> primaryResult = (WritePrimaryResult<
                    BulkShardRequest,
                    BulkShardResponse>) result;

                // Verify no location to sync (no writes occurred)
                assertNull(primaryResult.location);

                // Run post replication actions
                primaryResult.runPostReplicationActions(ActionListener.wrap(v -> {
                    // Success - refresh should not have been called
                }, e -> { fail("Post replication actions should not fail: " + e.getMessage()); }));

                // Verify refresh was NOT called even though refresh policy was IMMEDIATE
                assertFalse(refreshCalled.get());
            }), latch),
            threadPool,
            Names.WRITE
        );

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    public void testBulkRequestWithMixedSuccessAndFailureRefresh() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);

        // Create a mix of successful and failed operations
        BulkItemRequest[] items = new BulkItemRequest[3];

        // Item 0: Successful index operation
        items[0] = new BulkItemRequest(0, new IndexRequest("index").id("success1").source(Requests.INDEX_CONTENT_TYPE, "field", "value"));

        // Item 1: Failed update operation
        items[1] = new BulkItemRequest(1, new UpdateRequest("index", "fail1").doc(Requests.INDEX_CONTENT_TYPE, "field", "value"));

        // Item 2: Successful index operation
        items[2] = new BulkItemRequest(2, new IndexRequest("index").id("success2").source(Requests.INDEX_CONTENT_TYPE, "field", "value"));

        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.IMMEDIATE, items);

        IndexShard shard = mock(IndexShard.class);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);

        // Mock successful index operations
        Translog.Location resultLocation1 = new Translog.Location(42, 42, 42);
        Translog.Location resultLocation2 = new Translog.Location(43, 43, 43);
        Engine.IndexResult successResult1 = new FakeIndexResult(1, 1, 10, true, resultLocation1);
        Engine.IndexResult successResult2 = new FakeIndexResult(1, 1, 12, true, resultLocation2);

        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(
            successResult1
        ).thenReturn(successResult2);

        // Mock failed update operation
        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenThrow(
            new VersionConflictEngineException(shardId, "fail1", "version conflict")
        );

        // Track refresh calls
        AtomicBoolean refreshCalled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            refreshCalled.set(true);
            return null;
        }).when(shard).refresh(any());

        // Execute bulk operation
        CountDownLatch latch = new CountDownLatch(1);
        TransportShardBulkAction.performOnPrimary(
            bulkShardRequest,
            shard,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
                WritePrimaryResult<BulkShardRequest, BulkShardResponse> primaryResult = (WritePrimaryResult<
                    BulkShardRequest,
                    BulkShardResponse>) result;

                // Should have a location since some operations succeeded
                assertNotNull(primaryResult.location);

                // Run post replication actions
                primaryResult.runPostReplicationActions(ActionListener.wrap(v -> {
                    // Success
                }, e -> { fail("Post replication actions should not fail: " + e.getMessage()); }));

                // Verify refresh WAS called because there were successful writes
                assertTrue(refreshCalled.get());
            }), latch),
            threadPool,
            Names.WRITE
        );

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    public void testBulkRequestWithAllFailedUpdatesNoRefresh() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);

        // Create multiple failed update operations
        BulkItemRequest[] items = new BulkItemRequest[3];
        for (int i = 0; i < 3; i++) {
            items[i] = new BulkItemRequest(i, new UpdateRequest("index", "id" + i).doc(Requests.INDEX_CONTENT_TYPE, "field", "value"));
        }

        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.IMMEDIATE, items);

        IndexShard shard = mock(IndexShard.class);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);

        // Mock all updates to fail
        UpdateHelper updateHelper = mock(UpdateHelper.class);
        when(updateHelper.prepare(any(), eq(shard), any())).thenThrow(
            new VersionConflictEngineException(shardId, "id", "version conflict")
        );

        // Track refresh calls
        AtomicBoolean refreshCalled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            refreshCalled.set(true);
            return null;
        }).when(shard).refresh(any());

        // Execute bulk operation
        CountDownLatch latch = new CountDownLatch(1);
        TransportShardBulkAction.performOnPrimary(
            bulkShardRequest,
            shard,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
                WritePrimaryResult<BulkShardRequest, BulkShardResponse> primaryResult = (WritePrimaryResult<
                    BulkShardRequest,
                    BulkShardResponse>) result;

                // No location since all operations failed
                assertNull(primaryResult.location);

                // Run post replication actions
                primaryResult.runPostReplicationActions(ActionListener.wrap(v -> {
                    // Success
                }, e -> { fail("Post replication actions should not fail: " + e.getMessage()); }));

                // Verify refresh was NOT called
                assertFalse(refreshCalled.get());
            }), latch),
            threadPool,
            Names.WRITE
        );

        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    public void testSuccessfulBulkOperationStillTriggersRefresh() throws Exception {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);

        // Create successful operations
        BulkItemRequest[] items = new BulkItemRequest[2];
        items[0] = new BulkItemRequest(0, new IndexRequest("index").id("id1").source(Requests.INDEX_CONTENT_TYPE, "field", "value1"));
        items[1] = new BulkItemRequest(1, new IndexRequest("index").id("id2").source(Requests.INDEX_CONTENT_TYPE, "field", "value2"));

        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.IMMEDIATE, items);

        IndexShard shard = mock(IndexShard.class);
        when(shard.indexSettings()).thenReturn(indexSettings);
        when(shard.shardId()).thenReturn(shardId);

        // Mock successful operations
        AtomicInteger locationCounter = new AtomicInteger(42);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenAnswer(
            invocation -> {
                int loc = locationCounter.getAndIncrement();
                return new FakeIndexResult(1, 1, loc, true, new Translog.Location(loc, loc, loc));
            }
        );

        // Track refresh calls
        AtomicBoolean refreshCalled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            refreshCalled.set(true);
            return null;
        }).when(shard).refresh(any());

        // Execute bulk operation
        CountDownLatch latch = new CountDownLatch(1);
        TransportShardBulkAction.performOnPrimary(
            bulkShardRequest,
            shard,
            null,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
                WritePrimaryResult<BulkShardRequest, BulkShardResponse> primaryResult = (WritePrimaryResult<
                    BulkShardRequest,
                    BulkShardResponse>) result;

                // Should have location from successful operations
                assertNotNull(primaryResult.location);

                // Run post replication actions
                primaryResult.runPostReplicationActions(ActionListener.wrap(v -> {
                    // Success
                }, e -> { fail("Post replication actions should not fail: " + e.getMessage()); }));

                // Verify refresh WAS called
                assertTrue(refreshCalled.get());
            }), latch),
            threadPool,
            Names.WRITE
        );

        assertTrue(latch.await(5, TimeUnit.SECONDS));
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
        final String failureMessage = "simulated primary failure";
        final IOException exception = new IOException(failureMessage);
        BulkItemRequest itemRequest = new BulkItemRequest(
            0,
            new IndexRequest("index").source(Requests.INDEX_CONTENT_TYPE),
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
                BulkItemResponse primaryResponse = result.replicaRequest().items()[0].primaryResponse();
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

    public void testUpdateWithRetryOnConflict() throws IOException, InterruptedException {
        IndexSettings indexSettings = new IndexSettings(indexMetadata(), Settings.EMPTY);

        int nItems = randomIntBetween(2, 5);
        List<BulkItemRequest> items = new ArrayList<>(nItems);
        for (int i = 0; i < nItems; i++) {
            int retryOnConflictCount = randomIntBetween(0, 3);
            logger.debug("Setting retryCount for item {}: {}", i, retryOnConflictCount);
            UpdateRequest updateRequest = new UpdateRequest("index", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value")
                .retryOnConflict(retryOnConflictCount);
            items.add(new BulkItemRequest(i, updateRequest));
        }

        IndexRequest updateResponse = new IndexRequest("index").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value");

        Exception err = new VersionConflictEngineException(shardId, "id", "I'm conflicted <(;_;)>");
        Engine.IndexResult conflictedResult = new Engine.IndexResult(err, 0);

        IndexShard shard = mock(IndexShard.class);
        when(shard.applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenAnswer(
            ir -> conflictedResult
        );
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

        BulkShardRequest bulkShardRequest = new BulkShardRequest(shardId, RefreshPolicy.NONE, items.toArray(BulkItemRequest[]::new));

        BulkShardRequest[] completedRequest = new BulkShardRequest[1];
        final CountDownLatch latch = new CountDownLatch(1);
        Runnable runnable = () -> TransportShardBulkAction.performOnPrimary(
            bulkShardRequest,
            shard,
            updateHelper,
            threadPool::absoluteTimeInMillis,
            new NoopMappingUpdatePerformer(),
            listener -> listener.onResponse(null),
            new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
                assertEquals(nItems, result.replicaRequest().items().length);
                for (BulkItemRequest item : result.replicaRequest().items()) {
                    assertEquals(VersionConflictEngineException.class, item.primaryResponse().getFailure().getCause().getClass());
                }
                completedRequest[0] = result.replicaRequest();
            }), latch),
            threadPool,
            Names.WRITE
        );

        // execute the runnable on a separate thread so that the infinite loop can be detected
        new Thread(runnable).start();

        // timeout the request in 10 seconds if there is an infinite loop
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        for (BulkItemRequest item : completedRequest[0].items()) {
            assertEquals(item.primaryResponse().getFailure().getCause().getClass(), VersionConflictEngineException.class);

            // this assertion is based on the assumption that all bulk item requests are updates and are hence calling
            // UpdateRequest::prepareRequest
            UpdateRequest updateRequest = (UpdateRequest) item.request();
            verify(updateHelper, times(updateRequest.retryOnConflict() + 1)).prepare(
                eq(updateRequest),
                any(IndexShard.class),
                any(LongSupplier.class)
            );
        }
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

            items[0] = randomlySetIgnoredPrimaryResponse(items[0]);

            AtomicInteger updateCalled = new AtomicInteger();

            BulkShardRequest[] completedRequest = new BulkShardRequest[1];
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
                new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(result -> {
                    // Assert that we still need to fsync the location that was successfully written
                    assertThat(((WritePrimaryResult<BulkShardRequest, BulkShardResponse>) result).location, equalTo(resultLocation1));
                    completedRequest[0] = result.replicaRequest();
                }), latch),
                rejectingThreadPool,
                Names.WRITE
            );
            latch.await();

            assertThat("mappings were \"updated\" once", updateCalled.get(), equalTo(1));

            verify(shard, times(2)).applyIndexOperationOnPrimary(anyLong(), any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean());

            BulkItemResponse primaryResponse1 = completedRequest[0].items()[0].primaryResponse();
            assertThat(primaryResponse1.getItemId(), equalTo(0));
            assertThat(primaryResponse1.getId(), equalTo("id"));
            assertThat(primaryResponse1.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
            assertFalse(primaryResponse1.isFailed());
            assertThat(primaryResponse1.getResponse().status(), equalTo(RestStatus.CREATED));
            assertThat(primaryResponse1.getResponse().getSeqNo(), equalTo(10L));

            BulkItemResponse primaryResponse2 = completedRequest[0].items()[1].primaryResponse();
            assertThat(primaryResponse2.getItemId(), equalTo(1));
            assertThat(primaryResponse2.getId(), equalTo("id"));
            assertThat(primaryResponse2.getOpType(), equalTo(DocWriteRequest.OpType.INDEX));
            assertTrue(primaryResponse2.isFailed());
            assertNull(primaryResponse2.getResponse());
            assertEquals(RestStatus.TOO_MANY_REQUESTS, primaryResponse2.status());
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
            mock(RemoteStorePressureService.class),
            mock(SystemIndices.class),
            NoopTracer.INSTANCE
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
            mock(RemoteStorePressureService.class),
            mock(SystemIndices.class),
            NoopTracer.INSTANCE
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
            mock(RemoteStorePressureService.class),
            mock(SystemIndices.class),
            NoopTracer.INSTANCE
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
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(true));
        assertEquals(ReplicationMode.PRIMARY_TERM_VALIDATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeWithLocalTranslog() {
        TransportShardBulkAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(createIndexSettings(false));
        assertEquals(ReplicationMode.FULL_REPLICATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeWithRemoteStoreFencing() {
        // With the object-store fence enforcing stale-primary fencing on every (request-durability) translog upload,
        // replicas come off the write path entirely - no primary term validation fanout.
        TransportShardBulkAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(
            createIndexSettings(
                true,
                Settings.builder()
                    .put(IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED, true)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                    .build()
            )
        );
        assertEquals(ReplicationMode.NO_REPLICATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeWithRemoteStoreFencingAndAsyncDurability() {
        // ASYNC durability only validates the fence at the sync interval, so the per-operation term validation fanout
        // must be retained.
        TransportShardBulkAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(
            createIndexSettings(
                true,
                Settings.builder()
                    .put(IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED, true)
                    .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
                    .build()
            )
        );
        assertEquals(ReplicationMode.PRIMARY_TERM_VALIDATION, action.getReplicationMode(indexShard));
    }

    public void testGetReplicationModeWithFencingOnNonRemoteIndex() {
        // The setting is inert without remote store; fencing must not take replicas off the write path.
        TransportShardBulkAction action = createAction();
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(
            createIndexSettings(false, Settings.builder().put(IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED, true).build())
        );
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
            mock(RemoteStorePressureService.class),
            mock(SystemIndices.class),
            NoopTracer.INSTANCE
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

    private BulkItemRequest randomlySetIgnoredPrimaryResponse(BulkItemRequest primaryRequest) {
        if (randomBoolean()) {
            // add a response to the request and thereby check that it is ignored for the primary.
            return new BulkItemRequest(
                primaryRequest.id(),
                primaryRequest.request(),
                new BulkItemResponse(
                    0,
                    DocWriteRequest.OpType.INDEX,
                    new IndexResponse(shardId, "ignore-primary-response-on-primary", 42, 42, 42, false)
                )
            );
        }
        return primaryRequest;
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
