/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.action.termsenum.TermsEnumReducer.ShardResult;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.RemoteClusterAware;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Coordinates a terms_enum request: it checks the request against the cluster, sends one shard request per target
 * shard to a copy on a node that has the shard action, and answers the client once every shard has answered or the
 * request's timeout has passed, whichever comes first.
 * <p>
 * A scheduled timer owns the client-visible deadline. Shards that have not answered by then are reported as timed out,
 * and replies that arrive later are dropped. A failed shard request is retried on the next copy while time remains,
 * unless the task was cancelled.
 * <p>
 * The target shards are the ones a search through the same expression would query, alias search routing included, and
 * their number is capped by {@code action.search.shard_count.limit}. Unlike search, every shard request is sent at once:
 * there is no cap on concurrent shard requests per node.
 *
 * @opensearch.internal
 */
public class TransportTermsEnumAction extends HandledTransportAction<TermsEnumRequest, TermsEnumResponse>
    implements
        TransportIndicesResolvingAction<TermsEnumRequest> {

    private static final Logger logger = LogManager.getLogger(TransportTermsEnumAction.class);

    /** The name of the per-shard action, which runs on the {@code SEARCH} thread pool. */
    public static final String SHARD_ACTION_NAME = TermsEnumAction.NAME + "[s]";

    /** The largest {@code size} a request may ask for. */
    public static final Setting<Integer> MAX_SIZE_SETTING = Setting.intSetting(
        "search.terms_enum.max_size",
        1000,
        1,
        Property.Dynamic,
        Property.NodeScope
    );

    /** The first version whose nodes handle {@link #SHARD_ACTION_NAME}. */
    static final Version MIN_SHARD_VERSION = Version.V_3_11_0;

    /**
     * How long past the request's remaining time the transport layer keeps waiting for a shard reply. It only bounds
     * how long a response handler can stay registered for a node that never answers; the timer answers the client first.
     */
    static final long TRANSPORT_GRACE_MILLIS = 1000;

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndicesService indicesService;
    private final RemoteClusterService remoteClusterService;
    private final TermsEnumShardOperation shardOperation;
    private volatile int maxSize;

    /**
     * Creates the action and registers its shard handler.
     *
     * @param transportService sends and receives the shard requests
     * @param clusterService provides the cluster state and the dynamic settings
     * @param threadPool runs the shard work and the deadline timer
     * @param actionFilters the action filters
     * @param indexNameExpressionResolver resolves the target expressions
     * @param indicesService resolves local shards
     * @param searchService rules shards out through the index filter
     */
    @Inject
    public TransportTermsEnumAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        SearchService searchService
    ) {
        super(TermsEnumAction.NAME, transportService, actionFilters, TermsEnumRequest::new);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indicesService = indicesService;
        this.remoteClusterService = transportService.getRemoteClusterService();
        // the cached relative clock only ticks every 200 ms, too coarse for a budget of about a second
        this.shardOperation = new TermsEnumShardOperation(indicesService, searchService, threadPool::preciseRelativeTimeInNanos);
        this.maxSize = MAX_SIZE_SETTING.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_SIZE_SETTING, value -> this.maxSize = value);
        transportService.registerRequestHandler(SHARD_ACTION_NAME, ThreadPool.Names.SAME, TermsEnumShardRequest::new, this::onShardRequest);
    }

    @Override
    protected void doExecute(Task task, TermsEnumRequest request, ActionListener<TermsEnumResponse> listener) {
        final AsyncTermsEnum async;
        try {
            async = prepare((CancellableTask) task, request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        async.start();
    }

    /** Runs every check that fails the whole request before any shard is contacted, then plans the fan-out. */
    private AsyncTermsEnum prepare(CancellableTask task, TermsEnumRequest request, ActionListener<TermsEnumResponse> listener) {
        final int maxSize = this.maxSize;
        if (request.size() > maxSize) {
            throw new IllegalArgumentException(
                "[size] must be less than or equal to ["
                    + MAX_SIZE_SETTING.getKey()
                    + "] (["
                    + maxSize
                    + "]) but was ["
                    + request.size()
                    + "]"
            );
        }
        final ClusterState state = clusterService.state();
        state.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        final ResolvedIndices resolved = resolveIndices(request, state);
        if (resolved.remote().isEmpty() == false) {
            throw new IllegalArgumentException("[terms_enum] does not support cross-cluster requests");
        }
        final String[] concreteNames = ((ResolvedIndices.Local.Concrete) resolved.local()).namesOfConcreteIndicesAsArray();
        final ClusterBlockException blocked = state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteNames);
        if (blocked != null) {
            throw blocked;
        }
        rejectFilteredAliases(state, request, concreteNames);
        // an alias with search routing narrows the target shards, so this reaches the same shards as a search would
        final Map<String, Set<String>> routing = indexNameExpressionResolver.resolveSearchRouting(state, null, request.indices());
        final GroupShardsIterator<ShardIterator> shards = clusterService.operationRouting()
            .searchShards(state, concreteNames, routing, null);
        TransportSearchAction.failIfOverShardCountLimit(clusterService, shards.size());
        final List<ShardPlan> plans = planShards(shards, state.nodes());
        final long nowInMillis = System.currentTimeMillis();
        final long deadlineNanos = threadPool.preciseRelativeTimeInNanos() + request.timeout().nanos();
        return new AsyncTermsEnum(
            task,
            request,
            resolved.local().originalIndices(),
            state.nodes(),
            plans,
            nowInMillis,
            deadlineNanos,
            listener
        );
    }

    /**
     * Rejects targets reached through a filtered alias: terms would come from the whole index, not just the documents the
     * alias filter selects.
     */
    private void rejectFilteredAliases(ClusterState state, TermsEnumRequest request, String[] concreteNames) {
        final Set<String> expressions = indexNameExpressionResolver.resolveExpressions(state, request.indices());
        for (String index : concreteNames) {
            String[] aliases = indexNameExpressionResolver.filteringAliases(state, index, expressions);
            if (aliases != null) {
                throw new IllegalArgumentException(
                    "[terms_enum] does not support filtered aliases; index ["
                        + index
                        + "] is targeted through filtered alias ["
                        + aliases[0]
                        + "]"
                );
            }
        }
    }

    @Override
    public ResolvedIndices resolveIndices(TermsEnumRequest request) {
        return resolveIndices(request, clusterService.state());
    }

    private ResolvedIndices resolveIndices(TermsEnumRequest request, ClusterState state) {
        final Map<String, OriginalIndices> remoteClusterIndices = remoteClusterService.groupIndices(
            request.indicesOptions(),
            request.indices(),
            idx -> indexNameExpressionResolver.hasIndexAbstraction(idx, state)
        );
        final OriginalIndices localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        final ResolvedIndices.Local.Concrete concreteIndices;
        if (localIndices == null) {
            // only remote targets: do not expand to every local index
            concreteIndices = ResolvedIndices.Local.Concrete.empty();
        } else {
            // OriginalIndices includes data streams
            concreteIndices = indexNameExpressionResolver.concreteResolvedIndices(state, localIndices);
        }
        return ResolvedIndices.of(concreteIndices).withLocalOriginalIndices(localIndices).withRemoteIndices(remoteClusterIndices);
    }

    /**
     * Which copies of one shard may receive the shard request.
     *
     * @param shardId the shard
     * @param copies the copies on nodes that handle the shard action, in the order to try them
     * @param failure why the shard cannot be asked at all, or {@code null} when {@code copies} is not empty
     */
    record ShardPlan(ShardId shardId, List<ShardRouting> copies, @Nullable Exception failure) {
    }

    /**
     * Keeps, for every shard, the copies on known nodes that are on {@link #MIN_SHARD_VERSION} or later. A shard with no
     * such copy is planned as failed.
     *
     * @param shards the target shards, in shard order
     * @param nodes the nodes of the cluster state the shards come from
     * @return one plan per shard, in the same order
     * @throws IllegalArgumentException if there is at least one shard and every shard only has copies on older nodes
     */
    static List<ShardPlan> planShards(GroupShardsIterator<ShardIterator> shards, DiscoveryNodes nodes) {
        final List<ShardPlan> plans = new ArrayList<>(shards.size());
        int onlyOnOldNodes = 0;
        for (ShardIterator shard : shards) {
            final List<ShardRouting> copies = new ArrayList<>();
            boolean onOldNode = false;
            for (ShardRouting routing : shard.getShardRoutings()) {
                DiscoveryNode node = nodes.get(routing.currentNodeId());
                if (node == null) {
                    continue;
                }
                if (node.getVersion().onOrAfter(MIN_SHARD_VERSION)) {
                    copies.add(routing);
                } else {
                    onOldNode = true;
                }
            }
            final Exception failure;
            if (copies.isEmpty() == false) {
                failure = null;
            } else if (onOldNode) {
                onlyOnOldNodes++;
                failure = new IllegalStateException("no copy of this shard is on a node on version " + MIN_SHARD_VERSION + " or later");
            } else {
                failure = new NoShardAvailableActionException(shard.shardId());
            }
            plans.add(new ShardPlan(shard.shardId(), List.copyOf(copies), failure));
        }
        if (plans.isEmpty() == false && onlyOnOldNodes == plans.size()) {
            throw new IllegalArgumentException(
                "[terms_enum] requires nodes on version " + MIN_SHARD_VERSION + " or later, but no copy of any target shard is on one"
            );
        }
        return plans;
    }

    /**
     * The budget sent to a shard: the remaining time minus a reserve of {@code max(10 ms, 10%)}, so that a shard that
     * stops at its own deadline still answers before the coordinator does. Never less than 1 ms.
     *
     * @param remainingMillis the time left before the coordinator answers
     * @return the shard's budget in milliseconds, at least 1
     */
    static long shardBudgetMillis(long remainingMillis) {
        return Math.max(1, remainingMillis - Math.max(10, remainingMillis / 10));
    }

    /**
     * Whether a failed shard request should be retried on the next copy.
     *
     * @param e why the request failed
     * @param nextCopy the index of the copy that would be tried next
     * @param copyCount how many copies the shard has
     * @param remainingNanos the time left before the coordinator answers
     * @return true unless the task was cancelled, no copy is left or no time is left
     */
    static boolean shouldFailOver(Exception e, int nextCopy, int copyCount, long remainingNanos) {
        return ExceptionsHelper.unwrapCause(e) instanceof TaskCancelledException == false && nextCopy < copyCount && remainingNanos > 0;
    }

    /**
     * Handles a shard request. The shard's budget starts on receipt, so waiting for a pending refresh or in the
     * {@code SEARCH} queue counts against it.
     */
    private void onShardRequest(TermsEnumShardRequest request, TransportChannel channel, Task task) {
        final ActionListener<TermsEnumShardResponse> listener = new ChannelActionListener<>(channel, SHARD_ACTION_NAME, request);
        final long startNanos = threadPool.preciseRelativeTimeInNanos();
        try {
            final IndexShard shard = indicesService.indexServiceSafe(request.shardId().getIndex()).getShard(request.shardId().id());
            // a search-idle shard refreshes first, so the searcher acquired by the shard operation sees recent writes
            shard.awaitShardSearchActive(refreshed -> {
                try {
                    threadPool.executor(ThreadPool.Names.SEARCH)
                        .execute(
                            ActionRunnable.supply(listener, () -> shardOperation.execute(request, (CancellableTask) task, startNanos))
                        );
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * One request's fan-out. Each target shard owns one slot of {@link #results}, written exactly once: by the reply or
     * final failure of its last attempt, or by {@link #finish()} with a timeout. A slot is claimed with a compare-and-set
     * from {@code null}, so a reply racing the deadline either lands before the snapshot or is dropped.
     */
    private final class AsyncTermsEnum {
        private final CancellableTask task;
        private final TermsEnumRequest request;
        private final OriginalIndices localIndices;
        private final DiscoveryNodes nodes;
        private final List<ShardPlan> plans;
        private final long nowInMillis;
        private final long deadlineNanos;
        private final ActionListener<TermsEnumResponse> listener;
        private final AtomicReferenceArray<ShardResult> results;
        /** The number of slots with copies that have not been written yet. */
        private final AtomicInteger pending = new AtomicInteger();
        /** Set once by {@link #finish()}; guarantees a single answer to the client. */
        private final AtomicBoolean responded = new AtomicBoolean();
        private volatile Scheduler.ScheduledCancellable timer;

        AsyncTermsEnum(
            CancellableTask task,
            TermsEnumRequest request,
            OriginalIndices localIndices,
            DiscoveryNodes nodes,
            List<ShardPlan> plans,
            long nowInMillis,
            long deadlineNanos,
            ActionListener<TermsEnumResponse> listener
        ) {
            this.task = task;
            this.request = request;
            this.localIndices = localIndices;
            this.nodes = nodes;
            this.plans = plans;
            this.nowInMillis = nowInMillis;
            this.deadlineNanos = deadlineNanos;
            this.listener = listener;
            this.results = new AtomicReferenceArray<>(plans.size());
        }

        void start() {
            int withCopies = 0;
            for (int slot = 0; slot < plans.size(); slot++) {
                ShardPlan plan = plans.get(slot);
                if (plan.failure() != null) {
                    results.set(slot, ShardResult.failed(plan.shardId(), plan.failure()));
                } else {
                    withCopies++;
                }
            }
            // set before any send: replies decrement it
            pending.set(withCopies);
            if (withCopies == 0) {
                finish();
                return;
            }
            try {
                // round up so the timer never fires before the deadline
                long delayMillis = TimeUnit.NANOSECONDS.toMillis(Math.max(0, remainingNanos()) + TimeUnit.MILLISECONDS.toNanos(1) - 1);
                // assigned before the first send, so every finish() triggered by a reply sees it
                timer = threadPool.schedule(this::finish, TimeValue.timeValueMillis(delayMillis), ThreadPool.Names.GENERIC);
            } catch (Exception e) {
                if (responded.compareAndSet(false, true)) {
                    listener.onFailure(e);
                }
                return;
            }
            for (int slot = 0; slot < plans.size(); slot++) {
                if (plans.get(slot).failure() == null) {
                    send(slot, 0);
                }
            }
        }

        private long remainingNanos() {
            return deadlineNanos - threadPool.preciseRelativeTimeInNanos();
        }

        private void send(int slot, int copy) {
            if (responded.get()) {
                return;
            }
            final long remainingMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos());
            if (remainingMillis <= 0) {
                // the timer is due and records this shard as not having answered
                return;
            }
            final ShardPlan plan = plans.get(slot);
            final ShardRouting routing = plan.copies().get(copy);
            final DiscoveryNode node = nodes.get(routing.currentNodeId());
            final TermsEnumShardRequest shardRequest = new TermsEnumShardRequest(
                plan.shardId(),
                localIndices,
                request,
                nowInMillis,
                shardBudgetMillis(remainingMillis)
            );
            // a safety net against a node that never replies; the timer, not this timeout, answers the client
            final TransportRequestOptions options = TransportRequestOptions.builder()
                .withTimeout(remainingMillis + TRANSPORT_GRACE_MILLIS)
                .build();
            transportService.sendChildRequest(
                node,
                SHARD_ACTION_NAME,
                shardRequest,
                task,
                options,
                new TransportResponseHandler<TermsEnumShardResponse>() {
                    @Override
                    public TermsEnumShardResponse read(StreamInput in) throws IOException {
                        return new TermsEnumShardResponse(in);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(TermsEnumShardResponse response) {
                        record(slot, ShardResult.of(plan.shardId(), response));
                    }

                    @Override
                    public void handleException(TransportException e) {
                        onShardFailure(slot, copy, e);
                    }
                }
            );
        }

        private void onShardFailure(int slot, int copy, Exception e) {
            final ShardPlan plan = plans.get(slot);
            if (e instanceof ReceiveTimeoutTransportException) {
                // the transport timeout fires a full grace period after the deadline, so the shard did not answer in time
                record(slot, ShardResult.failed(plan.shardId(), notAnswered(e)));
                return;
            }
            if (shouldFailOver(e, copy + 1, plan.copies().size(), remainingNanos())) {
                logger.trace(
                    () -> new ParameterizedMessage("{}: retrying [{}] on the next copy", plan.copies().get(copy), SHARD_ACTION_NAME),
                    e
                );
                send(slot, copy + 1);
            } else {
                record(slot, ShardResult.failed(plan.shardId(), e));
            }
        }

        /** Records the final result of one shard, unless the client already has its answer. */
        private void record(int slot, ShardResult result) {
            if (responded.get()) {
                return;
            }
            if (results.compareAndSet(slot, null, result) && pending.decrementAndGet() == 0) {
                finish();
            }
        }

        private OpenSearchTimeoutException notAnswered(@Nullable Exception cause) {
            return new OpenSearchTimeoutException("shard did not respond within [{}]", cause, request.timeout());
        }

        /** Answers the client once, from whichever of the last reply and the timer comes first. */
        private void finish() {
            if (responded.compareAndSet(false, true) == false) {
                return;
            }
            final Scheduler.ScheduledCancellable timer = this.timer;
            if (timer != null) {
                timer.cancel();
            }
            if (task.isCancelled()) {
                listener.onFailure(new TaskCancelledException("cancelled task with reason: " + task.getReasonCancelled()));
                return;
            }
            final List<ShardResult> snapshot = new ArrayList<>(plans.size());
            for (int slot = 0; slot < plans.size(); slot++) {
                if (results.get(slot) == null) {
                    // claims the slot, so a reply that is still in flight can no longer land in it
                    results.compareAndSet(slot, null, ShardResult.failed(plans.get(slot).shardId(), notAnswered(null)));
                }
                snapshot.add(results.get(slot));
            }
            final TermsEnumResponse response;
            try {
                response = TermsEnumReducer.reduce(snapshot, request.size(), request.searchAfter());
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(response);
        }
    }
}
