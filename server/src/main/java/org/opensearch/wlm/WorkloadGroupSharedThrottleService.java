/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Cluster-level ({@code shared_limit}) throttle tier. Each throttle bucket has one authoritative in-flight counter on
 * the node that owns it per a consistent-hash ring ({@link ThrottleOwnerSelector}). This service is both the
 * coordinator-side client that acquires and releases shared permits and the owner-side host of the
 * {@link SharedThrottleTracker} for the buckets this node owns.
 * <p>
 * The acquire is asynchronous so the calling thread never blocks on the round trip; when the coordinator owns the
 * bucket itself it short-circuits to the local tracker with no network hop.
 * <p>
 * Fail-closed: if the owner gives no answer (timeout, disconnect, missing handler, empty ring), the caller is told the
 * shared tier is unavailable ({@link #isUnavailable}) and rejects the request (MONITOR still admits), so
 * {@code shared_limit} is never silently exceeded. The cost is that overflow for an unreachable owner's buckets is
 * rejected.
 * <p>
 * Release ordering: an acquire answered too late is reported unavailable by the coordinator's own timer, but its
 * response handler stays registered and releases a late grant when it arrives. A release is therefore only ever sent
 * after the owner granted, so it can never overtake the acquire and strand the permit. A permit whose release never
 * arrives is purged when its coordinator leaves the cluster, or else expires with its TTL.
 */
@ExperimentalApi
public class WorkloadGroupSharedThrottleService implements ClusterStateListener {

    static final String ACQUIRE_ACTION_NAME = "internal:wlm/throttle/shared/acquire";
    static final String RELEASE_ACTION_NAME = "internal:wlm/throttle/shared/release";

    // Fixed constants rather than cluster settings: internal coordination knobs an operator should not need to tune.
    //
    // How long a request waits for the owner's acquire reply before it is rejected as unavailable. Enforced by a
    // coordinator-side timer, NOT a transport timeout: the response handler outlives it (up to ACQUIRE_REPLY_BACKSTOP),
    // so a late grant is still seen and released after the owner granted it.
    static final TimeValue ACQUIRE_TIMEOUT = TimeValue.timeValueMillis(200);
    // Overrides ACQUIRE_TIMEOUT for tests. Deliberately not registered: a node rejects the key unless a test plugin
    // registers it, so it is not an operator setting.
    static final Setting<TimeValue> ACQUIRE_TIMEOUT_SETTING = Setting.timeSetting(
        "wlm.workload_group.throttle.shared_acquire_timeout",
        ACQUIRE_TIMEOUT,
        Setting.Property.NodeScope
    );
    // Transport timeout of the acquire's response handler. Only bounds how long a reply that never comes (half-open
    // connection) keeps the handler registered; a grant later than this falls back to the coordinator purge or the TTL.
    static final TimeValue ACQUIRE_REPLY_BACKSTOP = TimeValue.timeValueSeconds(30);
    // Timeout for the fire-and-forget RELEASE. Nothing waits on it and the TTL is the backstop, so it is long; it is
    // still bounded so a half-open connection can't pin the response handler.
    // Intentional: an owner stalled longer than these 30s windows makes TransportService log one late-response WARN per
    // pending acquire or release when it recovers. Accepted as rare rather than adding a per-owner fast-fail.
    static final TimeValue RELEASE_TIMEOUT = TimeValue.timeValueSeconds(30);
    // Time-to-live for a permit on the owner: the backstop for a permit whose release never arrives (lost release, a
    // grant later than ACQUIRE_REPLY_BACKSTOP, or a coordinator that was not purged). Well above typical request duration.
    // Accepted tradeoff: permits are NOT renewed, so a search running longer than the TTL loses its permit while still
    // executing and the bucket can transiently admit beyond shared_limit. If that becomes a problem, the fix is permit
    // renewal or a deadline-derived TTL, not a larger constant.
    static final long PERMIT_TTL_NANOS = TimeValue.timeValueMinutes(5).nanos();
    // How often the owner sweeps expired permits. Memory hygiene only: a saturated bucket already prunes its own expired
    // permits on acquire (see SharedThrottleTracker), so the sweep just drops expired records of idle buckets.
    static final TimeValue SWEEP_INTERVAL = TimeValue.timeValueMinutes(5);

    private static final Logger logger = LogManager.getLogger(WorkloadGroupSharedThrottleService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final SharedThrottleTracker tracker = new SharedThrottleTracker();
    private final TimeValue acquireTimeout;

    // Immutable ring snapshot, swapped wholesale so readers never see a half-built ring.
    private final AtomicReference<ThrottleOwnerSelector> ring = new AtomicReference<>();
    // Set once the first cluster-state event has been compared in full; only touched on the cluster applier thread.
    private boolean ringInitialized;
    private volatile Scheduler.Cancellable sweepTask;
    // Test seams, no-ops in production: after a successful tracker acquire (before the ownership re-check), and just
    // before a remote acquire is sent (its timer already scheduled).
    volatile Runnable afterTrackerAcquire = () -> {};
    volatile Consumer<RemoteAcquire> beforeRemoteAcquireSent = acquire -> {};

    public WorkloadGroupSharedThrottleService(ClusterService clusterService, ThreadPool threadPool, TransportService transportService) {
        this(clusterService, threadPool, transportService, ACQUIRE_TIMEOUT_SETTING.get(clusterService.getSettings()));
    }

    // Package-private: lets unit tests choose the acquire timeout.
    WorkloadGroupSharedThrottleService(
        ClusterService clusterService,
        ThreadPool threadPool,
        TransportService transportService,
        TimeValue acquireTimeout
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.acquireTimeout = acquireTimeout;
        // Start with an empty ring: there is no applied cluster state yet during node construction. The first
        // clusterChanged populates it (see there).
        this.ring.set(ThrottleOwnerSelector.fromDiscoveryNodes(DiscoveryNodes.EMPTY_NODES));
        clusterService.addListener(this);
        // Neither handler trips the in-flight breaker: the messages are tiny, and tripping it would turn one owner's
        // heap pressure into fail-closed 429s for every bucket it owns (and strand breaker-rejected releases until the TTL).
        transportService.registerRequestHandler(
            ACQUIRE_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            AcquirePermitRequest::new,
            (request, channel, task) -> channel.sendResponse(handleAcquire(request))
        );
        transportService.registerRequestHandler(
            RELEASE_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            ReleasePermitRequest::new,
            (request, channel, task) -> {
                tracker.release(request.bucketKey, request.permitId);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );
    }

    /** Starts the periodic TTL sweep. Call exactly once, at node start: a second call would schedule a duplicate sweep. */
    public void start() {
        // Does not touch the ring: the initial cluster state may not be applied yet at node start.
        sweepTask = threadPool.scheduleWithFixedDelay(() -> {
            try {
                tracker.sweepExpired();
            } catch (Exception e) {
                logger.warn("Shared throttle TTL sweep failed", e);
            }
        }, SWEEP_INTERVAL, ThreadPool.Names.GENERIC);
    }

    public void stop() {
        if (sweepTask != null) {
            sweepTask.cancel();
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // The first event must NOT be gated on nodesChanged(): the initial applied state already contains the local node,
        // so on a single-node (or otherwise stable) cluster nodesChanged() is never true and the ring would stay empty,
        // failing every shared acquire closed for the node's lifetime.
        // After that, an event without node changes cannot change the eligible set and is skipped. nodesChanged() compares
        // by ephemeral id, so a same-id restart still counts. Any future eligibility input that can change without a node
        // change must bypass this early return.
        if (ringInitialized && event.nodesChanged() == false) {
            return;
        }
        // A coordinator that left will never release its permits, so free them now rather than after the TTL. Removal is
        // by ephemeral id (a same-id restart purges the old incarnation) and independent of the ring comparison below,
        // since any removed node, whatever its roles, may have been a coordinator.
        if (event.nodesChanged() && event.nodesDelta().removed()) {
            final Set<String> removed = new HashSet<>();
            for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
                removed.add(node.getEphemeralId());
            }
            tracker.releaseAllFrom(removed);
        }
        // Compared by DiscoveryNode equality (ephemeral id) so a same-id restart replaces the stale node; rebuilt only when
        // the eligible set actually differs.
        Set<DiscoveryNode> candidate = ThrottleOwnerSelector.eligibleNodeSet(event.state().nodes());
        if (ring.get().eligibleNodeSet().equals(candidate) == false) {
            ring.set(ThrottleOwnerSelector.fromDiscoveryNodes(event.state().nodes()));
        }
        ringInitialized = true;
    }

    /**
     * Asynchronously acquires one shared permit for {@code bucketKey}, notifying {@code listener} with:
     * <ul>
     *   <li>{@code onResponse} with a {@link Releasable}: granted; close it when the request completes;</li>
     *   <li>{@code onFailure} with a message-less marker: {@link #isDenial} (bucket at its shared limit) or
     *       {@link #isUnavailable} (no authoritative answer from the owner). {@link WorkloadGroupService} composes the
     *       user-facing 429, since this service only knows the opaque bucket key;</li>
     *   <li>{@code onFailure} with any other {@link OpenSearchRejectedExecutionException}: the search pool rejected the
     *       hand-off of a grant, which was already released. Never delivered when {@code proceedsOnDenial} is true.</li>
     * </ul>
     * <p>
     * Threading: the local short-circuit notifies inline. A remote outcome after which the search continues (every grant,
     * and every outcome when {@code proceedsOnDenial}) is delivered on the {@link ThreadPool.Names#SEARCH} pool, so the
     * coordinator's pre-fan-out work does not run on the few network threads of one hot owner's connections. A refusal
     * that becomes a 429 is delivered inline, so a throttled tenant's overflow does not queue ahead of other tenants'
     * shard work (see {@code deliverRefusal}). If the search pool rejects the hand-off, refusals and MONITOR grants are
     * delivered inline; any other grant is released and the rejection passed through.
     *
     * @param proceedsOnDenial whether the caller continues the search even when denied or unavailable (MONITOR)
     */
    void acquireAsync(String bucketKey, int sharedLimit, boolean proceedsOnDenial, ActionListener<Releasable> listener) {
        final ThrottleOwnerSelector currentRing = ring.get();
        final DiscoveryNode owner = currentRing.ownerFor(bucketKey).orElse(null);
        if (owner == null) {
            listener.onFailure(unavailableMarker()); // empty ring: no owner to ask
            return;
        }

        final String permitId = permitId();

        // Local-owner short-circuit; the post-acquire fence re-reads the live ring.
        final DiscoveryNode localNode = clusterService.localNode();
        if (owner.equals(localNode)) {
            if (tracker.tryAcquire(bucketKey, sharedLimit, permitId, PERMIT_TTL_NANOS, localNode.getEphemeralId()) == false) {
                listener.onFailure(deniedMarker());
            } else if (fenceIfOwnershipLost(bucketKey, permitId)) {
                listener.onFailure(unavailableMarker());
            } else {
                listener.onResponse(releaseLocal(bucketKey, permitId));
            }
            return;
        }

        // Owner known to be disconnected (an in-memory lookup): report unavailable now rather than wait out the timer.
        if (transportService.nodeConnected(owner) == false) {
            listener.onFailure(unavailableMarker());
            return;
        }

        final ActionListener<Releasable> contextListener = ContextPreservingActionListener.wrapPreservingContext(
            listener,
            threadPool.getThreadContext()
        );
        final RemoteAcquire acquire = new RemoteAcquire(owner, bucketKey, permitId, proceedsOnDenial, contextListener);
        try {
            acquire.timer = threadPool.schedule(acquire::onTimeout, acquireTimeout, ThreadPool.Names.GENERIC);
        } catch (OpenSearchRejectedExecutionException e) {
            // The scheduler is shutting down: nothing was sent, so there is nothing to release.
            contextListener.onFailure(unavailableMarker());
            return;
        }
        beforeRemoteAcquireSent.accept(acquire);
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_REPLY_BACKSTOP).build();
        transportService.sendRequest(
            owner,
            ACQUIRE_ACTION_NAME,
            new AcquirePermitRequest(bucketKey, sharedLimit, permitId, PERMIT_TTL_NANOS, localNode.getEphemeralId()),
            options,
            acquire
        );
    }

    /**
     * One remote acquire. Its outcome is decided exactly once, by the first of the owner's reply, a transport failure or
     * the acquire timer; anything after that only cleans up (a late grant is released). Deciding drops the reference to
     * the caller's listener, so a handler still registered after the timer fired does not pin the answered request.
     */
    final class RemoteAcquire implements TransportResponseHandler<AcquirePermitResponse> {
        private final DiscoveryNode owner;
        private final String bucketKey;
        private final String permitId;
        private final boolean proceedsOnDenial;
        // The caller's listener until the outcome is decided, then null; claiming it is the decision.
        private final AtomicReference<ActionListener<Releasable>> listener;
        // Assigned before the request is sent, so every outcome sees it.
        private volatile Scheduler.ScheduledCancellable timer;

        RemoteAcquire(
            DiscoveryNode owner,
            String bucketKey,
            String permitId,
            boolean proceedsOnDenial,
            ActionListener<Releasable> listener
        ) {
            this.owner = owner;
            this.bucketKey = bucketKey;
            this.permitId = permitId;
            this.proceedsOnDenial = proceedsOnDenial;
            this.listener = new AtomicReference<>(listener);
        }

        // Returns the caller's listener, or null if already decided; cancels the timer.
        private ActionListener<Releasable> decide() {
            final ActionListener<Releasable> claimed = listener.getAndSet(null);
            if (claimed != null) {
                final Scheduler.ScheduledCancellable pending = timer;
                if (pending != null) {
                    pending.cancel();
                }
            }
            return claimed;
        }

        void onTimeout() {
            final ActionListener<Releasable> claimed = listener.getAndSet(null);
            if (claimed != null) {
                // Intentionally no release: it could overtake the in-flight acquire and strand a later grant. A late grant
                // is released by handleResponse once it arrives.
                logger.debug(
                    "Shared throttle acquire to owner [{}] for bucket [{}] timed out; shared tier unavailable",
                    owner.getId(),
                    bucketKey
                );
                deliverRefusal(claimed, unavailableMarker());
            }
        }

        // Visible for tests: whether the outcome is still undecided.
        boolean holdsListener() {
            return listener.get() != null;
        }

        // Visible for tests.
        Scheduler.ScheduledCancellable timer() {
            return timer;
        }

        @Override
        public AcquirePermitResponse read(StreamInput in) throws IOException {
            return new AcquirePermitResponse(in);
        }

        @Override
        public void handleResponse(AcquirePermitResponse response) {
            final ActionListener<Releasable> claimed = decide();
            if (claimed == null) {
                // Already reported unavailable by the timer: give a late grant straight back (safe, it follows the grant).
                if (response.granted) {
                    sendRelease(owner, bucketKey, permitId);
                }
                return;
            }
            if (response.granted) {
                final Releasable permit = releaseRemote(owner, bucketKey, permitId);
                deliverOnSearch(() -> claimed.onResponse(permit), rejection -> {
                    if (proceedsOnDenial) {
                        claimed.onResponse(permit);
                    } else {
                        permit.close();
                        claimed.onFailure(rejection);
                    }
                });
                return;
            }
            // not_owner: our ring and the owner's disagree mid-rebalance, so there is no authoritative answer.
            deliverRefusal(claimed, response.notOwner ? unavailableMarker() : deniedMarker());
        }

        @Override
        public void handleException(TransportException exp) {
            // A RemoteTransportException means a reply came back (an error, or one we could not read, which the native
            // transport also wraps): the owner already processed the acquire, so a release now follows any grant and is a
            // no-op otherwise. Any other failure gives no such ordering, so nothing is sent; the purge or TTL reclaims it.
            if (exp instanceof RemoteTransportException) {
                sendRelease(owner, bucketKey, permitId);
            }
            final ActionListener<Releasable> claimed = decide();
            if (claimed != null) {
                logger.debug(
                    "Shared throttle acquire to owner [{}] for bucket [{}] failed; shared tier unavailable",
                    owner.getId(),
                    bucketKey
                );
                deliverRefusal(claimed, unavailableMarker());
            }
        }

        // Inline when the caller turns it into a 429, otherwise on the search pool (inline if the pool rejects it).
        // Intentional: an inline 429 runs on the network or timer thread, so a caller that starts new work from it (_msearch's
        // next sub-search) may run that work's pre-fan-out there. Rare, and not worth a thread hop on every denial.
        private void deliverRefusal(ActionListener<Releasable> claimed, Exception marker) {
            if (proceedsOnDenial) {
                deliverOnSearch(() -> claimed.onFailure(marker), rejection -> claimed.onFailure(marker));
            } else {
                claimed.onFailure(marker);
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    // Runs outcome on the search pool; onRejection runs inline on the calling thread if the pool rejects it.
    private void deliverOnSearch(Runnable outcome, Consumer<Exception> onRejection) {
        threadPool.executor(ThreadPool.Names.SEARCH).execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                outcome.run();
            }

            @Override
            public void onRejection(Exception e) {
                onRejection.accept(e);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Unexpected failure delivering shared throttle acquire outcome", e);
            }
        });
    }

    // Owner-side admission. Package-private for tests.
    //
    // Former-owner fence: a coordinator still on an old ring may keep routing here after the bucket moved, and granting
    // would count against a stale counter while the new owner counts from zero, so a non-owner refuses with not_owner.
    // The check is repeated after the acquire to narrow the race with a concurrent ring swap.
    // Accepted: a coordinator removed from the cluster but still able to reach this owner (asymmetric partition) can be
    // granted permits after its purge; those fall back to the TTL. Rare.
    AcquirePermitResponse handleAcquire(AcquirePermitRequest request) {
        if (isStillOwner(request.bucketKey) == false) {
            return AcquirePermitResponse.NOT_OWNER;
        }
        // Intentional: the owner trusts the coordinator's limit rather than re-resolving it, so while a shared_limit change
        // propagates, coordinators that have not applied it yet enforce the old value. Short and self-correcting.
        if (tracker.tryAcquire(
            request.bucketKey,
            request.sharedLimit,
            request.permitId,
            request.ttlNanos,
            request.coordinatorId
        ) == false) {
            return AcquirePermitResponse.DENIED;
        }
        return fenceIfOwnershipLost(request.bucketKey, request.permitId) ? AcquirePermitResponse.NOT_OWNER : AcquirePermitResponse.GRANTED;
    }

    // Re-checks ownership after a successful tracker acquire; if the bucket moved in between, returns the permit and true.
    private boolean fenceIfOwnershipLost(String bucketKey, String permitId) {
        afterTrackerAcquire.run();
        if (isStillOwner(bucketKey)) {
            return false;
        }
        tracker.release(bucketKey, permitId);
        return true;
    }

    private boolean isStillOwner(String bucketKey) {
        return isLocalOwner(ring.get(), bucketKey);
    }

    // DiscoveryNode equality is by ephemeral id, the identity the ring is rebuilt on.
    private boolean isLocalOwner(ThrottleOwnerSelector selector, String bucketKey) {
        final DiscoveryNode localNode = clusterService.localNode();
        return selector.ownerFor(bucketKey).filter(localNode::equals).isPresent();
    }

    private Releasable releaseLocal(String bucketKey, String permitId) {
        return releaseOnce(() -> tracker.release(bucketKey, permitId));
    }

    // Closing does not wait for the owner to apply the release (see WorkloadGroupService#releaseThrottlePermitBeforeCompletion).
    private Releasable releaseRemote(DiscoveryNode owner, String bucketKey, String permitId) {
        return releaseOnce(() -> sendRelease(owner, bucketKey, permitId));
    }

    // Fire-and-forget RELEASE to the bucket owner. A lost release is not fatal (the permit expires with its TTL), so
    // failures are only logged at debug.
    private void sendRelease(DiscoveryNode owner, String bucketKey, String permitId) {
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(RELEASE_TIMEOUT).build();
        transportService.sendRequest(
            owner,
            RELEASE_ACTION_NAME,
            new ReleasePermitRequest(bucketKey, permitId),
            options,
            new TransportResponseHandler<TransportResponse.Empty>() {
                @Override
                public TransportResponse.Empty read(StreamInput in) {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {}

                @Override
                public void handleException(TransportException exp) {
                    // Accepted: not retried. If the connection to the owner briefly drops while both nodes stay in the
                    // cluster, the permit stays counted until its TTL, which can cause temporary false 429s for the bucket.
                    logger.debug(
                        "Shared throttle release to owner [{}] for bucket [{}] failed; TTL will reclaim",
                        owner.getId(),
                        bucketKey
                    );
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }

    // Guards against a double close (e.g. onRequestEnd and onRequestFailure) releasing twice.
    private static Releasable releaseOnce(Runnable release) {
        final AtomicBoolean released = new AtomicBoolean(false);
        return () -> {
            if (released.compareAndSet(false, true)) {
                release.run();
            }
        };
    }

    private static String permitId() {
        return UUIDs.base64UUID();
    }

    // Message-less markers; WorkloadGroupService composes the user-facing 429. "Denied": the bucket is at its shared limit.
    private static OpenSearchRejectedExecutionException deniedMarker() {
        return new DeniedMarkerException();
    }

    /** Whether {@code e} is this service's shared-limit denial, as opposed to e.g. a search-pool rejection. */
    static boolean isDenial(Exception e) {
        return e instanceof DeniedMarkerException;
    }

    // "Unavailable": no answer could be obtained from the bucket's owner.
    private static RuntimeException unavailableMarker() {
        return new UnavailableMarkerException();
    }

    /** Whether {@code e} means the shared tier could not answer (empty ring, owner unreachable, timeout, not_owner). */
    static boolean isUnavailable(Exception e) {
        return e instanceof UnavailableMarkerException;
    }

    private static final class UnavailableMarkerException extends RuntimeException {
        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    private static final class DeniedMarkerException extends OpenSearchRejectedExecutionException {
        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    /**
     * Raw number of permit records this node holds for {@code bucketKey}, not filtered by expiry or ownership. Public
     * only for cross-package tests ({@code WlmClusterThrottlingIT}).
     */
    public int ownedInFlight(String bucketKey) {
        return tracker.inFlight(bucketKey);
    }

    // Visible for tests.
    SharedThrottleTracker tracker() {
        return tracker;
    }

    ThrottleOwnerSelector ring() {
        return ring.get();
    }

    /**
     * Each RPC body below is one name-keyed map rather than positional fields, so peers built from different commits of
     * the same release (e.g. a blue/green deployment, which a {@code Version} gate cannot tell apart) stay
     * interoperable: unknown keys are ignored.
     * <p>
     * Reading rules: baseline fields are read strictly ({@code require*}); a malformed message then fails while
     * decoding, as a positional one would. A field added later must be read with a safe default if a same-version peer
     * may lack it, or behind a stream-version gate otherwise (the stream version is the negotiated minimum, so an older
     * peer never takes the strict branch). Use only types {@code readGenericValue} knows (String, Integer, Long, Boolean,
     * List, Map).
     */
    private static Map<String, Object> readBody(StreamInput in) throws IOException {
        return in.readMap(StreamInput::readString, StreamInput::readGenericValue);
    }

    // Baseline fields: strict.

    private static String requireString(Map<String, Object> body, String key) {
        final Object value = body.get(key);
        if (value instanceof String s) {
            return s;
        }
        throw new IllegalStateException("wlm shared-throttle: key [" + key + "] missing or not a String [" + value + "]");
    }

    private static Number requireNumber(Map<String, Object> body, String key) {
        final Object value = body.get(key);
        if (value instanceof Number n) {
            return n;
        }
        throw new IllegalStateException("wlm shared-throttle: key [" + key + "] missing or not a Number [" + value + "]");
    }

    private static boolean requireBoolean(Map<String, Object> body, String key) {
        final Object value = body.get(key);
        if (value instanceof Boolean b) {
            return b;
        }
        throw new IllegalStateException("wlm shared-throttle: key [" + key + "] missing or not a Boolean [" + value + "]");
    }

    // Fields added after the baseline: a same-version peer may omit them, so read with a default.

    private static boolean optionalBoolean(Map<String, Object> body, String key, boolean defaultValue) {
        final Object value = body.get(key);
        return value instanceof Boolean b ? b : defaultValue;
    }

    private static String optionalString(Map<String, Object> body, String key, String defaultValue) {
        final Object value = body.get(key);
        return value instanceof String s ? s : defaultValue;
    }

    /** {@code coord -> owner}: admit one request under {@code sharedLimit}. */
    static final class AcquirePermitRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_SHARED_LIMIT = "shared_limit";
        static final String KEY_PERMIT_ID = "permit_id";
        static final String KEY_TTL_NANOS = "ttl_nanos";
        // Added after the baseline: the coordinator's ephemeral id, for the purge on its removal. A peer that omits it
        // reads as UNKNOWN_COORDINATOR, whose permits fall back to the TTL.
        static final String KEY_COORDINATOR = "coordinator_node_id";

        final String bucketKey;
        final int sharedLimit;
        final String permitId;
        final long ttlNanos;
        final String coordinatorId;

        AcquirePermitRequest(String bucketKey, int sharedLimit, String permitId, long ttlNanos, String coordinatorId) {
            this.bucketKey = bucketKey;
            this.sharedLimit = sharedLimit;
            this.permitId = permitId;
            this.ttlNanos = ttlNanos;
            this.coordinatorId = coordinatorId;
        }

        AcquirePermitRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.sharedLimit = requireNumber(body, KEY_SHARED_LIMIT).intValue();
            this.permitId = requireString(body, KEY_PERMIT_ID);
            this.ttlNanos = requireNumber(body, KEY_TTL_NANOS).longValue();
            this.coordinatorId = optionalString(body, KEY_COORDINATOR, SharedThrottleTracker.UNKNOWN_COORDINATOR);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(
                Map.of(
                    KEY_BUCKET,
                    bucketKey,
                    KEY_SHARED_LIMIT,
                    sharedLimit,
                    KEY_PERMIT_ID,
                    permitId,
                    KEY_TTL_NANOS,
                    ttlNanos,
                    KEY_COORDINATOR,
                    coordinatorId
                ),
                StreamOutput::writeString,
                StreamOutput::writeGenericValue
            );
        }
    }

    /** {@code owner -> coord}: granted, denied, or {@code not_owner} (the answering node no longer owns the bucket). */
    static final class AcquirePermitResponse extends TransportResponse {
        static final String KEY_GRANTED = "granted";
        // Added after the baseline: written only when true and read with a default of false, so a peer that predates it
        // reads not_owner as a plain denial.
        static final String KEY_NOT_OWNER = "not_owner";

        static final AcquirePermitResponse GRANTED = new AcquirePermitResponse(true, false);
        static final AcquirePermitResponse DENIED = new AcquirePermitResponse(false, false);
        static final AcquirePermitResponse NOT_OWNER = new AcquirePermitResponse(false, true);

        final boolean granted;
        final boolean notOwner;

        AcquirePermitResponse(boolean granted, boolean notOwner) {
            assert (granted && notOwner) == false : "a grant cannot also be a not-owner refusal";
            this.granted = granted;
            this.notOwner = notOwner;
        }

        AcquirePermitResponse(StreamInput in) throws IOException {
            final Map<String, Object> body = readBody(in);
            this.granted = requireBoolean(body, KEY_GRANTED);
            this.notOwner = optionalBoolean(body, KEY_NOT_OWNER, false);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            final Map<String, Object> body = notOwner ? Map.of(KEY_GRANTED, granted, KEY_NOT_OWNER, true) : Map.of(KEY_GRANTED, granted);
            out.writeMap(body, StreamOutput::writeString, StreamOutput::writeGenericValue);
        }
    }

    /** {@code coord -> owner}: fire-and-forget, drop a previously granted permit. */
    static final class ReleasePermitRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_PERMIT_ID = "permit_id";

        final String bucketKey;
        final String permitId;

        ReleasePermitRequest(String bucketKey, String permitId) {
            this.bucketKey = bucketKey;
            this.permitId = permitId;
        }

        ReleasePermitRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.permitId = requireString(body, KEY_PERMIT_ID);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(
                Map.of(KEY_BUCKET, bucketKey, KEY_PERMIT_ID, permitId),
                StreamOutput::writeString,
                StreamOutput::writeGenericValue
            );
        }
    }
}
