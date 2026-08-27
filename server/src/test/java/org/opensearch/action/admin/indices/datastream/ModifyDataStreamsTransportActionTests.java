/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataDataStreamsService;
import org.opensearch.cluster.metadata.MetadataDataStreamsService.ModifyDataStreamsClusterStateUpdateRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for the transport half of {@link ModifyDataStreamsAction}: executor choice, response reading,
 * cluster-block handling and delegation to {@link MetadataDataStreamsService}.
 */
public class ModifyDataStreamsTransportActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private CapturingMetadataDataStreamsService dataStreamsService;
    private ModifyDataStreamsAction.TransportAction action;

    /**
     * Records the update request handed to the service so the test can assert on delegation without running a real
     * cluster-state update, and lets each test choose whether the service succeeds or fails.
     */
    private static class CapturingMetadataDataStreamsService extends MetadataDataStreamsService {

        private final AtomicReference<ModifyDataStreamsClusterStateUpdateRequest> captured = new AtomicReference<>();
        private volatile boolean acknowledged = true;
        private volatile RuntimeException failure = null;

        CapturingMetadataDataStreamsService(ClusterService clusterService) {
            super(clusterService);
        }

        @Override
        public void modifyDataStream(ModifyDataStreamsClusterStateUpdateRequest request, ActionListener<AcknowledgedResponse> listener) {
            captured.set(request);
            if (failure != null) {
                listener.onFailure(failure);
            } else {
                listener.onResponse(new AcknowledgedResponse(acknowledged));
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("ModifyDataStreamsTransportActionTests");
        clusterService = createClusterService(threadPool);
        CapturingTransport capturingTransport = new CapturingTransport();
        transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        dataStreamsService = new CapturingMetadataDataStreamsService(clusterService);
        action = new ModifyDataStreamsAction.TransportAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(clusterService.getSettings())),
            dataStreamsService
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    /**
     * The action is metadata-only and cheap, so it runs on the calling thread rather than a dedicated pool.
     */
    public void testExecutorIsSame() {
        assertThat(action.executor(), equalTo(ThreadPool.Names.SAME));
    }

    /**
     * read() reconstructs an acknowledged response from the wire, for both acknowledged values.
     */
    public void testReadResponse() throws IOException {
        for (boolean acknowledged : new boolean[] { true, false }) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                new AcknowledgedResponse(acknowledged).writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    AcknowledgedResponse response = action.read(in);
                    assertThat(response.isAcknowledged(), equalTo(acknowledged));
                }
            }
        }
    }

    /**
     * With no global blocks in place checkBlock returns null, so the operation proceeds.
     */
    public void testCheckBlockPassesWithoutBlocks() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        assertNull(action.checkBlock(newRequest(), state));
    }

    /**
     * A global METADATA_WRITE block (cluster read-only) is surfaced as a ClusterBlockException.
     */
    public void testCheckBlockRejectsMetadataWriteBlock() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .blocks(ClusterBlocks.builder().addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK).build())
            .build();
        ClusterBlockException e = action.checkBlock(newRequest(), state);
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("cluster read-only (api)"));
    }

    /**
     * A global block that does not include METADATA_WRITE (create-index only) does not block this action.
     */
    public void testCheckBlockIgnoresUnrelatedBlock() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .blocks(ClusterBlocks.builder().addGlobalBlock(Metadata.CLUSTER_CREATE_INDEX_BLOCK).build())
            .build();
        assertNull(action.checkBlock(newRequest(), state));
    }

    /**
     * clusterManagerOperation forwards the request's actions and both timeouts to the metadata service, and passes the
     * service's acknowledgement back to the listener.
     */
    public void testClusterManagerOperationDelegatesToService() {
        List<DataStreamAction> actions = Arrays.asList(
            DataStreamAction.addBackingIndex("logs-foo", ".ds-logs-foo-000001"),
            DataStreamAction.removeBackingIndex("logs-bar", ".ds-logs-bar-000002")
        );
        ModifyDataStreamsAction.Request request = new ModifyDataStreamsAction.Request(actions);
        request.timeout(TimeValue.timeValueSeconds(13));
        request.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(17));

        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        action.clusterManagerOperation(
            request,
            ClusterState.builder(ClusterName.DEFAULT).build(),
            ActionListener.wrap(responseRef::set, e -> {
                throw new AssertionError("unexpected failure", e);
            })
        );

        ModifyDataStreamsClusterStateUpdateRequest captured = dataStreamsService.captured.get();
        assertThat(captured, notNullValue());
        assertThat(captured.getActions(), contains(actions.toArray()));
        assertThat(captured.ackTimeout(), equalTo(TimeValue.timeValueSeconds(13)));
        assertThat(captured.masterNodeTimeout(), equalTo(TimeValue.timeValueSeconds(17)));

        assertThat(responseRef.get(), notNullValue());
        assertTrue(responseRef.get().isAcknowledged());
    }

    /**
     * A non-acknowledged service result is relayed unchanged rather than being coerced to true.
     */
    public void testClusterManagerOperationRelaysNotAcknowledged() {
        dataStreamsService.acknowledged = false;

        AtomicReference<AcknowledgedResponse> responseRef = new AtomicReference<>();
        action.clusterManagerOperation(
            newRequest(),
            ClusterState.builder(ClusterName.DEFAULT).build(),
            ActionListener.wrap(responseRef::set, e -> {
                throw new AssertionError("unexpected failure", e);
            })
        );

        assertThat(responseRef.get(), notNullValue());
        assertFalse(responseRef.get().isAcknowledged());
    }

    /**
     * A failure raised by the metadata service reaches the caller's listener untouched.
     */
    public void testClusterManagerOperationPropagatesFailure() {
        dataStreamsService.failure = new IllegalArgumentException("data stream [logs-foo] not found");

        AtomicReference<Exception> failureRef = new AtomicReference<>();
        action.clusterManagerOperation(newRequest(), ClusterState.builder(ClusterName.DEFAULT).build(), ActionListener.wrap(r -> {
            throw new AssertionError("expected failure but got " + r);
        }, failureRef::set));

        assertThat(failureRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(failureRef.get().getMessage(), containsString("data stream [logs-foo] not found"));
    }

    private static ModifyDataStreamsAction.Request newRequest() {
        return new ModifyDataStreamsAction.Request(
            Collections.singletonList(DataStreamAction.addBackingIndex("logs-foo", ".ds-logs-foo-000001"))
        );
    }
}
