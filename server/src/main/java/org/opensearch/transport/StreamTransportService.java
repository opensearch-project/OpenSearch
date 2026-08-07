/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.opensearch.discovery.HandshakingTransportAddressConnector.PROBE_CONNECT_TIMEOUT_SETTING;
import static org.opensearch.discovery.HandshakingTransportAddressConnector.PROBE_HANDSHAKE_TIMEOUT_SETTING;

/**
 * Transport service for streaming requests, handling StreamTransportResponse.
 * @opensearch.experimental
 */
@ExperimentalApi
public class StreamTransportService extends TransportService {
    private static final Logger logger = LogManager.getLogger(StreamTransportService.class);
    public static final Setting<TimeValue> STREAM_TRANSPORT_REQ_TIMEOUT_SETTING = Setting.timeSetting(
        "transport.stream.request_timeout",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Bounded in-flight batch queue depth for the local (in-process) streaming path — the number of
     * response batches a producer may stage ahead of the consumer before {@code sendResponseBatch}
     * blocks (the local-path substitute for the wire path's gRPC-readiness backpressure). It bounds the
     * per-stream resident off-heap working set (≈ {@code depth × batch_size}); the aggregate across
     * streams is this times the concurrent-stream count, which the caller governs separately (e.g. a
     * per-node shard-request concurrency limit). Deliberately NOT scaled by cores: a wider machine
     * drives more concurrent streams, not a deeper per-stream pipeline, so scaling depth by cores would
     * multiply resident memory without adding intra-stream pipelining. Default 4; raise only when the
     * consumer's memory budget can absorb the extra resident batches.
     */
    public static final Setting<Integer> STREAM_TRANSPORT_LOCAL_QUEUE_DEPTH_SETTING = Setting.intSetting(
        "transport.stream.local.queue_depth",
        4,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile TimeValue streamTransportReqTimeout;
    private volatile int localStreamQueueDepth;

    public StreamTransportService(
        Settings settings,
        Transport streamTransport,
        ThreadPool threadPool,
        TransportInterceptor transportInterceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        TaskManager taskManager,
        RemoteClusterService remoteClusterService,
        Tracer tracer
    ) {
        super(
            settings,
            streamTransport,
            threadPool,
            transportInterceptor,
            localNodeFactory,
            // it's a single channel profile and let underlying client handle parallelism by creating multiple channels as needed
            new ClusterConnectionManager(
                ConnectionProfile.buildSingleChannelProfile(
                    TransportRequestOptions.Type.STREAM,
                    PROBE_CONNECT_TIMEOUT_SETTING.get(settings),
                    PROBE_HANDSHAKE_TIMEOUT_SETTING.get(settings),
                    TimeValue.MINUS_ONE,
                    false
                ),
                streamTransport
            ),
            tracer,
            taskManager,
            remoteClusterService,
            true
        );
        this.streamTransportReqTimeout = STREAM_TRANSPORT_REQ_TIMEOUT_SETTING.get(settings);
        this.localStreamQueueDepth = STREAM_TRANSPORT_LOCAL_QUEUE_DEPTH_SETTING.get(settings);
        if (clusterSettings != null) {
            clusterSettings.addSettingsUpdateConsumer(STREAM_TRANSPORT_REQ_TIMEOUT_SETTING, this::setStreamTransportReqTimeout);
            clusterSettings.addSettingsUpdateConsumer(STREAM_TRANSPORT_LOCAL_QUEUE_DEPTH_SETTING, v -> this.localStreamQueueDepth = v);
        }
    }

    @Override
    public <T extends TransportResponse> void sendChildRequest(
        final Transport.Connection connection,
        final String action,
        final TransportRequest request,
        final Task parentTask,
        final TransportResponseHandler<T> handler
    ) {
        sendChildRequest(
            connection,
            action,
            request,
            parentTask,
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).withTimeout(streamTransportReqTimeout).build(),
            handler
        );
    }

    @Override
    public void connectToNode(final DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Void> listener) {
        if (isLocalNode(node)) {
            listener.onResponse(null);
            return;
        }
        // TODO: add logic for validation
        final ActionListener<Void> wrappedListener = ActionListener.wrap(response -> { listener.onResponse(response); }, exception -> {
            logger.warn("Failed to connect to streaming node [{}]: {}", node, exception.getMessage());
            listener.onFailure(new ConnectTransportException(node, "Failed to connect for streaming", exception));
        });

        connectionManager.connectToNode(
            node,
            connectionProfile,
            (connection, profile, listener1) -> listener1.onResponse(null),
            wrappedListener
        );
    }

    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
        // Same-node streaming requests skip the (loopback) wire: dispatch the registered handler
        // in-process and hand its response batches to the caller by reference, avoiding the
        // serialize -> transport -> deserialize round-trip.
        //
        // We match on the persistent node id rather than isLocalNode(): the stream transport resolves
        // its own local node via a separate LocalNodeFactory (different random ephemeralId), so
        // isLocalNode() — which compares ephemeralId — never matches the cluster-state node here.
        // Matching by node id also keeps this decision independent of isLocalNode()'s other role in
        // connectToNode(), which suppresses establishing a loopback self-connection; that self-
        // connection is what any non-local-dispatch code path still relies on.
        if (localNode != null && node != null && localNode.getId().equals(node.getId())) {
            return new LocalStreamConnection(localNode);
        }
        try {
            return connectionManager.getConnection(node);
        } catch (Exception e) {
            logger.error("Failed to get streaming connection to node [{}]: {}", node, e.getMessage());
            throw new ConnectTransportException(node, "Failed to get streaming connection", e);
        }
    }

    /**
     * In-process {@link Transport.Connection} for the local node. Instead of writing the request to a
     * (loopback) channel, it runs the registered request handler directly against a
     * {@link LocalStreamChannel} whose response batches are delivered to the caller's
     * {@link StreamTransportResponseHandler#handleStreamResponse} without serialization.
     *
     * <p>This is the streaming analogue of the unary in-process path core {@link TransportService}
     * already provides ({@code localNodeConnection} -&gt; {@code sendLocalRequest} -&gt;
     * {@code DirectResponseChannel}). It intentionally shares that contract: the request object is
     * handed to the handler as-is (no serialize/deserialize round-trip — there is no wire to validate,
     * so any input constraints must live in handler logic, exactly as for the unary local path today);
     * and the connection is a per-request synthetic handle that is never pooled or reused, so like
     * {@code localNodeConnection} it has a no-op {@code close()}/{@code addCloseListener} and reports
     * {@code isClosed() == false}. It is discarded after the single {@code sendRequest}.
     */
    private final class LocalStreamConnection implements Transport.Connection {
        private final DiscoveryNode node;

        LocalStreamConnection(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public DiscoveryNode getNode() {
            return node;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) {
            sendLocalStreamRequest(requestId, action, request, options);
        }

        @Override
        public void close() {}

        @Override
        public void addCloseListener(ActionListener<Void> listener) {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public org.opensearch.Version getVersion() {
            return node.getVersion();
        }
    }

    /**
     * Runs the registered handler for {@code action} in-process on the request executor, feeding its
     * streamed response batches to the caller's response handler via a bounded, backpressured queue —
     * the same producer-blocks-when-full flow control the wire path gets from gRPC readiness.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void sendLocalStreamRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) {
        final RequestHandlerRegistry reg = getRequestHandler(action);
        if (reg == null) {
            deliverLocalStreamException(requestId, action, new ActionNotFoundTransportException("Action [" + action + "] not found"));
            return;
        }
        // Capture the depth once per stream: a dynamic update applies to newly dispatched streams,
        // not ones already draining.
        final LocalStreamChannel channel = new LocalStreamChannel(localNode, requestId, action, localStreamQueueDepth);
        final String executor = reg.getExecutor();
        final AbstractRunnable task = new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                reg.processMessageReceived(request, channel);
            }

            @Override
            public boolean isForceExecution() {
                return reg.isForceExecution();
            }

            @Override
            public void onFailure(Exception e) {
                channel.onProducerError(e);
            }
        };
        // Producer runs on the handler's executor (GENERIC when the handler registered SAME, since the
        // consumer blocks draining nextResponse() and producing on the same thread would deadlock).
        final Executor producerExecutor = ThreadPool.Names.SAME.equals(executor)
            ? getThreadPool().executor(ThreadPool.Names.GENERIC)
            : getThreadPool().executor(executor);
        producerExecutor.execute(task);
        // Guard: onResponseSent must be invoked exactly once per requestId regardless of outcome.
        final AtomicBoolean responseSentFired = new AtomicBoolean(false);
        // Deliver the stream to the caller's handler on a separate GENERIC thread rather than inline, so
        // this sendRequest returns immediately — mirroring the async wire path where connection.sendRequest
        // hands off and returns. Draining inline would block the caller's dispatch thread for the whole
        // stream, serializing sibling requests (e.g. per-shard fragments dispatched in a loop) that the
        // wire path runs concurrently.
        getThreadPool().executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                deliverLocalStream(requestId, action, channel, responseSentFired);
            }

            @Override
            public void onFailure(Exception e) {
                channel.cancel("local stream delivery failed", e);
                if (responseSentFired.compareAndSet(false, true)) {
                    deliverLocalStreamException(requestId, action, e);
                }
            }
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void deliverLocalStream(long requestId, String action, LocalStreamChannel channel, AtomicBoolean responseSentFired) {
        if (responseSentFired.compareAndSet(false, true)) {
            onResponseSent(requestId, action, TransportResponse.Empty.INSTANCE);
        }
        final TransportResponseHandler handler = responseHandlers.onResponseReceived(requestId, this);
        if (handler == null) {
            channel.cancel("no response handler registered for local stream", null);
            return;
        }
        // handleStreamResponse is a default method on every TransportResponseHandler (streaming
        // consumers override it), so call it directly — the same entry point the wire path uses.
        // We deliberately do NOT require the StreamTransportResponseHandler marker subinterface:
        // callers such as the analytics fragment handler implement streaming on a plain
        // TransportResponseHandler and would otherwise be rejected.
        try {
            handler.handleStreamResponse(channel.responseStream());
        } catch (Exception e) {
            channel.cancel("local stream handling failed", e);
            handler.handleException(new TransportException("local stream handling failed for action [" + action + "]", e));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void deliverLocalStreamException(long requestId, String action, Exception e) {
        onResponseSent(requestId, action, e);
        final TransportResponseHandler handler = responseHandlers.onResponseReceived(requestId, this);
        if (handler != null) {
            handler.handleException(new TransportException(e));
        }
    }

    private void setStreamTransportReqTimeout(TimeValue streamTransportReqTimeout) {
        this.streamTransportReqTimeout = streamTransportReqTimeout;
    }

    /**
     * The configured streaming request timeout ({@code transport.stream.request_timeout}). Callers
     * that build their own {@link TransportRequestOptions} (rather than using the
     * {@link #sendChildRequest} overload that injects it) should apply this so streaming requests
     * honor the same timeout the service applies by default.
     */
    public TimeValue getStreamTransportReqTimeout() {
        return streamTransportReqTimeout;
    }

    /**
     * A {@link TransportChannel} for in-process streaming. The producer (the request handler) calls
     * {@link #sendResponseBatch}/{@link #completeStream}/{@link #sendResponse(Exception)}; those items
     * are placed on a bounded queue. The consumer drains them through {@link #responseStream()}'s
     * {@link StreamTransportResponse#nextResponse()}, which blocks when the queue is empty and thereby
     * backpressures the producer when full — mirroring the wire path's flow control, without any
     * serialization. Batches are passed by reference; ownership transfers to the consumer exactly as
     * on the wire path (the consumer closes each response it receives).
     *
     * <p>Package-private so its producer/consumer backpressure semantics can be unit-tested directly.
     */
    static final class LocalStreamChannel implements TransportChannel {
        private final DiscoveryNode localNode;
        private final long requestId;
        private final String action;
        // Sentinels distinguish end-of-stream and error from real batches on the single queue.
        private final Object endOfStream = new Object();
        private final BlockingQueue<Object> queue;
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        // Two-phase terminal state: terminated means no more items will be enqueued; completionPending
        // is set when completeStream() wins the CAS but has not yet enqueued the sentinel (it may be
        // blocking on a full queue). This allows a subsequent error to override a pending completion.
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private volatile boolean completionPending = false;
        private volatile boolean consumerCancelled = false;
        // Set once the consumer has observed end-of-stream (or an error) so that nextResponse() is
        // idempotent at the terminal: further calls return null immediately instead of blocking on
        // queue.take() for a sentinel that was already consumed.
        private volatile boolean consumerDrained = false;

        LocalStreamChannel(DiscoveryNode localNode, long requestId, String action, int queueDepth) {
            this.localNode = localNode;
            this.requestId = requestId;
            this.action = action;
            this.queue = new ArrayBlockingQueue<>(queueDepth);
        }

        @Override
        public String getProfileName() {
            return DIRECT_RESPONSE_PROFILE;
        }

        @Override
        public String getChannelType() {
            return "direct-stream";
        }

        @Override
        public org.opensearch.Version getVersion() {
            return localNode.getVersion();
        }

        @Override
        public void sendResponseBatch(TransportResponse response) {
            if (consumerCancelled) {
                releaseIfPossible(response);
                return;
            }
            try {
                // Blocks when the queue is full — this is the producer-side backpressure.
                queue.put(response);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                onProducerError(new TransportException("interrupted while enqueuing local stream batch", e));
                return;
            }
            // Re-check after enqueue: cancel() may have drained the queue between our pre-check and
            // the put(), leaving this batch stranded. Remove and release it so off-heap buffers don't
            // leak. queue.remove is O(n) but cancel is rare (early termination / error) and the queue
            // is bounded to a small depth.
            if (consumerCancelled && queue.remove(response)) {
                releaseIfPossible(response);
            }
        }

        @Override
        public void completeStream() {
            if (terminated.compareAndSet(false, true)) {
                completionPending = true;
                // Block until space is available — the consumer is actively draining, so all previously
                // enqueued batches will be delivered before this sentinel. Unlike the error path
                // (enqueueTerminalError), normal completion must not drop batches.
                try {
                    queue.put(endOfStream);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                completionPending = false;
            }
        }

        @Override
        public void sendResponse(TransportResponse response) {
            // Non-streaming single response: deliver as one batch then complete.
            sendResponseBatch(response);
            completeStream();
        }

        @Override
        public void sendResponse(Exception exception) {
            if (terminated.compareAndSet(false, true)) {
                failure.set(exception);
                enqueueTerminalError(exception);
            } else if (completionPending) {
                // completeStream() won the CAS but is blocked waiting to enqueue the end-of-stream
                // sentinel. Override with the error so the consumer sees the failure rather than a
                // clean completion. The error terminal force-drains, which will unblock the completion
                // put() (it will fail to insert since queue.put is interruptible or will see the queue
                // drained and insert harmlessly after the error — either way the consumer already got
                // the error sentinel first).
                failure.set(exception);
                enqueueTerminalError(exception);
            }
        }

        /** Producer-side failure (handler threw before/without a terminal). */
        void onProducerError(Exception e) {
            if (terminated.compareAndSet(false, true)) {
                failure.set(e);
                enqueueTerminalError(e);
            } else if (completionPending) {
                failure.set(e);
                enqueueTerminalError(e);
            }
        }

        private void enqueueTerminalError(Object terminal) {
            // Error terminal must not be lost even if the queue is full: drop undelivered batches so the
            // error surfaces to the consumer promptly. Only used for error paths — normal completion
            // uses blocking put() to preserve all batches.
            while (queue.offer(terminal) == false) {
                Object dropped = queue.poll();
                releaseIfPossible(dropped);
            }
        }

        private void releaseIfPossible(Object item) {
            // A batch reaches here only when it was enqueued but never handed to the consumer (the
            // consumer cancelled/closed, or the producer kept sending after cancel). The consumer owns
            // and closes every batch it actually takes from the queue; these undelivered ones would
            // otherwise leak their off-heap buffers. The generic transport can't see the concrete
            // (Arrow) type, so it honors the generic AutoCloseable contract — the batch response type
            // implements it to free its buffers, mirroring the wire path's releaseUnsentSource. The
            // queue is the single ownership arbiter (poll() removes exactly once), so this can never
            // double-close a batch the consumer already claimed.
            if (item instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage("failed to release undelivered local stream batch for action [{}]", action),
                        e
                    );
                }
            }
        }

        void cancel(String reason, Throwable cause) {
            consumerCancelled = true;
            terminated.set(true);
            // Drain and release anything the producer already enqueued.
            Object item;
            while ((item = queue.poll()) != null) {
                releaseIfPossible(item);
            }
        }

        @SuppressWarnings("unchecked")
        <T extends TransportResponse> StreamTransportResponse<T> responseStream() {
            return new StreamTransportResponse<T>() {
                @Override
                public T nextResponse() {
                    // Terminal is sticky: once the consumer has drained end-of-stream, errored, or
                    // cancelled, return null without touching the queue. Otherwise a second call
                    // would block forever on queue.take() waiting for a sentinel already consumed.
                    if (consumerCancelled || consumerDrained) {
                        return null;
                    }
                    try {
                        Object item = queue.take(); // blocks until producer supplies a batch/terminal
                        if (item == endOfStream) {
                            consumerDrained = true;
                            return null;
                        }
                        if (item instanceof Exception e) {
                            consumerDrained = true;
                            throw new TransportException("local stream producer failed for action [" + action + "]", e);
                        }
                        return (T) item;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new TransportException("interrupted while draining local stream", e);
                    }
                }

                @Override
                public void cancel(String reason, Throwable cause) {
                    LocalStreamChannel.this.cancel(reason, cause);
                }

                @Override
                public void close() {
                    // Release any undrained batches the producer left behind.
                    Object item;
                    while ((item = queue.poll()) != null) {
                        releaseIfPossible(item);
                    }
                }
            };
        }
    }
}
