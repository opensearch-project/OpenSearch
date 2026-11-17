/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.forcemerge;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.autoforcemerge.AutoForceMergeManager;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportForceMergeActionTests extends OpenSearchTestCase {

    private IndicesService indicesService;
    private AutoForceMergeManager autoForceMergeManager;
    private IndexShard indexShard;
    private ShardRouting shardRouting;
    private TransportForceMergeAction action;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        indicesService = mock(IndicesService.class);
        autoForceMergeManager = mock(AutoForceMergeManager.class);
        indexShard = mock(IndexShard.class);

        ShardId shardId = new ShardId("test-index", "_na_", 0);
        shardRouting = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
        IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(shardId.getIndex())).thenReturn(indexService);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);

        action = new TransportForceMergeAction(
            mock(ClusterService.class),
            mock(TransportService.class),
            indicesService,
            new ActionFilters(Collections.emptySet()),
            mock(IndexNameExpressionResolver.class),
            autoForceMergeManager
        );
    }

    public void testShardOperationRejectedWhenNodeInDuress() throws Exception {
        when(autoForceMergeManager.shouldRejectForceMerge()).thenReturn(true);
        ForceMergeRequest request = new ForceMergeRequest("test-index");

        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> action.shardOperation(request, shardRouting)
        );
        // The rejection status is what failOnDuressRejection turns into a request-level 429.
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(e));
        assertTrue(e.getMessage(), e.getMessage().contains("[test-index][0]"));
        assertTrue(e.getMessage(), e.getMessage().contains("ignore_duress=true"));
        verify(indexShard, never()).forceMerge(any());
        verifyNoInteractions(indicesService);
    }

    public void testShardOperationRunsWhenIgnoreDuressSet() throws Exception {
        when(autoForceMergeManager.shouldRejectForceMerge()).thenReturn(true);
        ForceMergeRequest request = new ForceMergeRequest("test-index").ignoreDuress(true);

        action.shardOperation(request, shardRouting);

        verify(indexShard).forceMerge(request);
        // The override short-circuits before consulting the node-duress check.
        verify(autoForceMergeManager, never()).shouldRejectForceMerge();
    }

    public void testShardOperationRunsWhenNodeNotInDuress() throws Exception {
        when(autoForceMergeManager.shouldRejectForceMerge()).thenReturn(false);
        ForceMergeRequest request = new ForceMergeRequest("test-index");

        action.shardOperation(request, shardRouting);

        verify(autoForceMergeManager).shouldRejectForceMerge();
        verify(indexShard).forceMerge(request);
    }

    public void testRequestFailsWith429WhenShardRejectedForDuress() {
        // the rejection arrives from the data node wrapped in a RemoteTransportException
        final ForceMergeResponse response = new ForceMergeResponse(
            2,
            1,
            1,
            List.of(
                new DefaultShardOperationFailedException(
                    "test-index",
                    0,
                    new RemoteTransportException("[node-2]", new OpenSearchRejectedExecutionException("node is under duress"))
                )
            )
        );

        final Exception e = completeWith(response).failure.get();

        assertThat(e, instanceOf(OpenSearchRejectedExecutionException.class));
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(e));
        assertThat(e.getMessage(), containsString("[1] of [2] shards"));
        assertThat(e.getMessage(), containsString("node is under duress"));
    }

    public void testResponsePassedThroughWithoutDuressRejection() {
        final ForceMergeResponse response = new ForceMergeResponse(
            2,
            1,
            1,
            List.of(new DefaultShardOperationFailedException("test-index", 0, new IllegalStateException("not a rejection")))
        );

        final Outcome outcome = completeWith(response);

        assertThat(outcome.response.get(), sameInstance(response));
        assertNull(outcome.failure.get());
    }

    public void testFailurePassedThrough() {
        final Outcome outcome = new Outcome();
        final IllegalStateException failure = new IllegalStateException("boom");

        TransportForceMergeAction.failOnDuressRejection(outcome.listener()).onFailure(failure);

        assertThat(outcome.failure.get(), sameInstance(failure));
        assertNull(outcome.response.get());
    }

    public void testRequestFailsWith429CountingEveryDuressRejection() {
        final int rejected = randomIntBetween(2, 5);
        final int succeeded = randomIntBetween(0, 3);
        final List<DefaultShardOperationFailedException> failures = new ArrayList<>();
        for (int shard = 0; shard < rejected; shard++) {
            failures.add(
                new DefaultShardOperationFailedException(
                    "test-index",
                    shard,
                    new OpenSearchRejectedExecutionException("node is under duress")
                )
            );
        }

        final Exception e = completeWith(new ForceMergeResponse(rejected + succeeded, succeeded, rejected, failures)).failure.get();

        assertThat(e, instanceOf(OpenSearchRejectedExecutionException.class));
        assertThat(e.getMessage(), containsString("[" + rejected + "] of [" + (rejected + succeeded) + "] shards"));
    }

    public void testRequestFailsWith429WhenDuressRejectionIsMixedWithOtherFailures() {
        final ForceMergeResponse response = new ForceMergeResponse(
            3,
            1,
            2,
            List.of(
                new DefaultShardOperationFailedException("test-index", 0, new IllegalStateException("not a rejection")),
                new DefaultShardOperationFailedException("test-index", 1, new OpenSearchRejectedExecutionException("node is under duress"))
            )
        );

        final Exception e = completeWith(response).failure.get();

        assertEquals(RestStatus.TOO_MANY_REQUESTS, ExceptionsHelper.status(e));
        assertThat(e.getMessage(), containsString("[1] of [3] shards"));
    }

    public void testSuccessfulResponsePassedThrough() {
        final ForceMergeResponse response = new ForceMergeResponse(2, 2, 0, List.of());

        final Outcome outcome = completeWith(response);

        assertThat(outcome.response.get(), sameInstance(response));
        assertNull(outcome.failure.get());
    }

    private static Outcome completeWith(ForceMergeResponse response) {
        final Outcome outcome = new Outcome();
        TransportForceMergeAction.failOnDuressRejection(outcome.listener()).onResponse(response);
        return outcome;
    }

    private static final class Outcome {
        final AtomicReference<ForceMergeResponse> response = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();

        ActionListener<ForceMergeResponse> listener() {
            return ActionListener.wrap(response::set, failure::set);
        }
    }
}
