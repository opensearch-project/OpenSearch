/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.QueryExecution;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.shuffle.ShuffleBufferManager;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.BroadcastInjectionInstructionNode;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Dispatcher for Variant A distributed aggregation over a cascade hash-shuffle join (q5/q10). Sits
 * between {@link BroadcastDispatch} (two-pass build-then-probe) and {@link CascadeShuffleDispatch}
 * (recursive worker tiers): it builds the tiny dimension tables once, then runs the cascade with a
 * {@code PARTIAL} aggregate on the top worker that has the dimension data broadcast into it.
 *
 * <p><b>Phase 1 — build the dimensions.</b> {@link DistributedAggOverJoinRewriter} tagged each
 * dimension reducer stage {@link Stage.StageRole#BROADCAST_BUILD}. For each, run its sub-graph in
 * isolation (exactly as {@link BroadcastDispatch} pass 1) into an Arrow-IPC capture sink and collect
 * the bytes keyed by {@code namedInputId = "broadcast-<dimStageId>"}.
 *
 * <p><b>Phase 2 — inject + run the cascade.</b> Append one
 * {@link BroadcastInjectionInstructionNode} per dimension to the top worker's plan alternatives (so
 * the data-node worker handler registers each {@code broadcast-<id>} memtable before executing the
 * fragment — see {@code AnalyticsSearchService.executeWorkerFragmentStreamingAsync}, which runs the
 * instruction chain for worker fragments just like the probe path), enrich every cascade shuffle
 * level's producer/scan/worker instructions (reusing {@link CascadeShuffleDispatch#enrichLevels}),
 * strip the already-built dimension stages from the top worker's children, and hand the DAG to the
 * scheduler. The cascade worker then runs, per partition,
 * {@code Aggregate(PARTIAL) → dim-joins(broadcast memtables) → bottom-join(shuffle, shuffle)} and
 * gathers its partials to the coordinator's {@code Aggregate(FINAL) → Sort}.
 *
 * <p><b>Status (HONEST).</b> The rewrite this dispatcher drives is JVM-tested
 * ({@code DistributedAggOverJoinTests}); the dispatch itself (IPC capture, worker injection, cascade
 * enrichment) is execution-only and validated by sf=10 TPC-H q5/q10, NOT by JVM tests. The
 * structural feasibility was verified by file:line trace (worker request carries plan-alternative
 * instructions; the worker handler runs them; the DataFusion convertor resolves {@code broadcast-<id>}
 * NamedScans inside a worker session).
 *
 * @opensearch.internal
 */
public final class DistributedAggOverJoinDispatch {

    private static final Logger LOGGER = LogManager.getLogger(DistributedAggOverJoinDispatch.class);

    /** Tripwire for the capture-sink contract (mirrors {@link BroadcastDispatch}). */
    private static final long EXTRACT_IPC_TIMEOUT_SECONDS = 30L;

    private final StageExecutionBuilder stageExecutionBuilder;
    private final QueryScheduler scheduler;
    private final ClusterService clusterService;
    private final ShuffleBufferManager shuffleBufferManager;
    private final CapabilityRegistry capabilityRegistry;
    private final boolean preferMetadataDriver;

    public DistributedAggOverJoinDispatch(
        StageExecutionBuilder stageExecutionBuilder,
        QueryScheduler scheduler,
        ClusterService clusterService,
        ShuffleBufferManager shuffleBufferManager,
        CapabilityRegistry capabilityRegistry,
        boolean preferMetadataDriver
    ) {
        this.stageExecutionBuilder = stageExecutionBuilder;
        this.scheduler = scheduler;
        this.clusterService = clusterService;
        this.shuffleBufferManager = shuffleBufferManager;
        this.capabilityRegistry = capabilityRegistry;
        this.preferMetadataDriver = preferMetadataDriver;
    }

    /**
     * Drives the distributed-agg-over-cascade query end to end.
     *
     * @param captureSinkFactory creates the backend IPC capture sink for a given build stage's
     *     output rowType — identical contract to {@link BroadcastDispatch} (the sink exposes
     *     {@code CompletableFuture<byte[]> ipcBytesFuture()}, completed by {@code close()}).
     */
    public void run(
        QueryContext ctx,
        QueryDAG dag,
        Function<Stage, ExchangeSink> captureSinkFactory,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> rawTerminal
    ) {
        // Once-only guard around the terminal: multiple dimension-build state listeners can each fire
        // onFailure (two dims failing, or a SUCCEEDED racing a FAILED), and phase 2 also completes the
        // terminal — without this, terminal.onFailure/onResponse could be invoked more than once.
        // (codex review SHOULD-FIX: no once-only terminal guard across build listeners.)
        AtomicBoolean done = new AtomicBoolean(false);
        ActionListener<Iterable<VectorSchemaRoot>> terminal = new ActionListener<>() {
            @Override
            public void onResponse(Iterable<VectorSchemaRoot> result) {
                if (done.compareAndSet(false, true)) {
                    rawTerminal.onResponse(result);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (done.compareAndSet(false, true)) {
                    rawTerminal.onFailure(e);
                }
            }
        };
        try {
            // 1. Rewrite into the distributed-agg shape (PARTIAL on the top worker, FINAL on the
            // coordinator, dims → broadcast scans) and run the full convert pipeline. The cascade
            // structure inside drives the per-level shuffle enrichment.
            DistributedAggOverJoinRewriter.Structure structure = DistributedAggOverJoinRewriter.rewrite(
                dag,
                capabilityRegistry,
                preferMetadataDriver,
                (levelIndex, partitionCount) -> resolveTargetWorkerNodeIds(partitionCount)
            );
            QueryDAG rewrittenDag = structure.dag();
            List<DistributedAggOverJoinRewriter.BroadcastBuild> builds = structure.broadcastBuilds();
            if (builds.isEmpty()) {
                throw new IllegalStateException("DistributedAggOverJoinDispatch: rewrite produced no dimension builds");
            }

            // The top worker stage lives in the rewritten DAG — locate it by id (the cascade
            // structure's top level is the last in bottom-up order).
            List<CascadeShuffleDAGRewriter.WorkerLevel> levels = structure.cascade().buildLevels();
            Stage topWorker = levels.get(levels.size() - 1).worker();

            // 2. Phase 1: build each dimension in isolation + capture IPC, then phase 2.
            buildDimensionsThenRun(ctx, rewrittenDag, topWorker, levels, builds, captureSinkFactory, queryExecutionSink, terminal);
        } catch (Exception e) {
            terminal.onFailure(e);
        }
    }

    /**
     * Builds every dimension stage sequentially (each into its own capture sink), collecting the
     * IPC bytes, then runs phase 2. Builds run one at a time so a build failure short-circuits before
     * the (expensive) cascade dispatch; the dimension tables are tiny so serial build is cheap.
     */
    private void buildDimensionsThenRun(
        QueryContext ctx,
        QueryDAG rewrittenDag,
        Stage topWorker,
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels,
        List<DistributedAggOverJoinRewriter.BroadcastBuild> builds,
        Function<Stage, ExchangeSink> captureSinkFactory,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        Map<String, byte[]> capturedByName = new LinkedHashMap<>();
        AtomicInteger remaining = new AtomicInteger(builds.size());
        // Collect every build sub-graph so we can install ONE cancel callback covering all of them
        // before scheduling any work. Scheduling happens only after the cancel wiring is in place.
        List<StageExecution> buildRoots = new ArrayList<>(builds.size());
        List<StageExecution> allLeaves = new ArrayList<>();
        for (DistributedAggOverJoinRewriter.BroadcastBuild build : builds) {
            Stage buildStage = build.buildStage();
            ExchangeSink captureSink = captureSinkFactory.apply(buildStage);
            StageExecutionBuilder.SubGraph buildGraph = stageExecutionBuilder.buildSubGraphWithSink(
                buildStage,
                captureSink,
                ctx,
                scheduler::scheduleStage
            );
            StageExecution buildExec = buildGraph.root();
            buildRoots.add(buildExec);
            allLeaves.addAll(buildGraph.leaves());
            // Listener MUST be installed before the cancel callback below: setOnCancelCallback replays
            // synchronously if the task is already cancelled, and that replay drives buildExec to
            // CANCELLED — a listener registered after that point would miss the transition and the
            // terminal would never fire (mirrors the ordering contract in BroadcastDispatch).
            buildExec.addStateListener((from, to) -> {
                switch (to) {
                    case SUCCEEDED -> {
                        byte[] ipc;
                        try {
                            captureSink.close();
                            ipc = extractIpcBytes(captureSink);
                        } catch (Throwable t) {
                            LOGGER.warn(
                                new org.apache.logging.log4j.message.ParameterizedMessage(
                                    "[DistributedAggOverJoinDispatch] dimension build capture failed for {}",
                                    build.namedInputId()
                                ),
                                t
                            );
                            terminal.onFailure(new RuntimeException("DistributedAggOverJoinDispatch: dimension build capture failed", t));
                            return;
                        }
                        synchronized (capturedByName) {
                            capturedByName.put(build.namedInputId(), ipc);
                        }
                        if (remaining.decrementAndGet() == 0) {
                            // The query may have been cancelled while the dimension builds ran; don't
                            // start the (expensive) cascade dispatch in that case. Mirrors
                            // BroadcastDispatch's pass-1→pass-2 cancel guard. (codex review SHOULD-FIX.)
                            if (ctx.parentTask() != null && ctx.parentTask().isCancelled()) {
                                String reason = ctx.parentTask().getReasonCancelled() != null
                                    ? ctx.parentTask().getReasonCancelled()
                                    : "unknown";
                                terminal.onFailure(new TaskCancelledException("query cancelled during dimension builds: " + reason));
                                return;
                            }
                            // All dimensions captured — proceed to phase 2.
                            try {
                                startPhase2(ctx, rewrittenDag, topWorker, levels, capturedByName, queryExecutionSink, terminal);
                            } catch (Exception e) {
                                terminal.onFailure(e);
                            }
                        }
                    }
                    case FAILED -> {
                        try {
                            captureSink.close();
                        } catch (Throwable ignore) {
                            // primary failure surfaced below
                        }
                        Exception cause = buildExec.getFailure();
                        // Cancel sibling dimension builds BEFORE firing the terminal: the terminal closes
                        // the shared per-query allocator, and a still-running sibling build's capture sink
                        // would otherwise keep serializing Arrow IPC against the freed allocator (a
                        // use-after-free). (codex review round-3 SHOULD-FIX.) The once-only terminal guard
                        // ensures only this first failure surfaces; redundant cancels are no-ops.
                        cancelOtherBuilds(buildRoots, buildExec, "sibling dimension build failed");
                        terminal.onFailure(
                            cause != null ? cause : new RuntimeException("dimension build stage " + buildStage.getStageId() + " FAILED")
                        );
                    }
                    case CANCELLED -> {
                        try {
                            captureSink.close();
                        } catch (Throwable ignore) {
                            // primary cancel surfaced below
                        }
                        cancelOtherBuilds(buildRoots, buildExec, "sibling dimension build cancelled");
                        terminal.onFailure(new RuntimeException("dimension build stage " + buildStage.getStageId() + " CANCELLED"));
                    }
                    default -> {
                        // CREATED / RUNNING — no action.
                    }
                }
            });
        }

        // Wire cancellation for the build phase AFTER every listener is installed. The default query
        // lifecycle installs an onCancel callback in QueryScheduler.execute, but phase 1 bypasses that
        // path (it schedules build leaves directly), so a user cancel or cancel_after_time_interval
        // during a build would otherwise be silently dropped until all builds finished. Cancel every
        // build execution; phase 2's QueryScheduler.execute replaces this callback with its own
        // walker-level cancel. (codex review round-2 SHOULD-FIX: build phase had no cancel callback.)
        //
        // If the task is already cancelled, setOnCancelCallback replays synchronously: each build
        // cancels, its (already-installed) listener fires CANCELLED, and the terminal resolves.
        AnalyticsQueryTask parentTask = ctx.parentTask();
        if (parentTask != null) {
            parentTask.setOnCancelCallback(() -> {
                String reason = parentTask.getReasonCancelled() != null ? parentTask.getReasonCancelled() : "unknown";
                LOGGER.debug("[DistributedAggOverJoinDispatch] build phase cancel requested, reason={}", reason);
                for (StageExecution buildExec : buildRoots) {
                    try {
                        buildExec.cancel("task cancelled: " + reason);
                    } catch (Exception e) {
                        LOGGER.warn("[DistributedAggOverJoinDispatch] failed to cancel build exec", e);
                    }
                }
            });
        }

        // If the cancel callback already drove the builds to a terminal state via the listeners, don't
        // schedule — the terminal has already resolved (mirrors BroadcastDispatch's pre-start guard).
        if (parentTask != null && parentTask.isCancelled()) {
            LOGGER.debug("[DistributedAggOverJoinDispatch] task already cancelled before build start; not scheduling builds");
            return;
        }

        for (StageExecution leaf : allLeaves) {
            scheduler.scheduleStage(leaf);
        }
    }

    /**
     * Cancels every build execution except {@code self}. Called on the first phase-1 build failure /
     * cancel, before the terminal fires, so no sibling build keeps running (and serializing IPC against
     * the soon-to-be-closed shared allocator) after the query has already failed. Best-effort: a sibling
     * already in a terminal state ignores the cancel; the once-only terminal guard means only the first
     * failure surfaces to the caller. (codex review round-3 SHOULD-FIX.)
     */
    private static void cancelOtherBuilds(List<StageExecution> buildRoots, StageExecution self, String reason) {
        for (StageExecution other : buildRoots) {
            if (other == self) {
                continue;
            }
            try {
                other.cancel(reason);
            } catch (Exception e) {
                LOGGER.warn("[DistributedAggOverJoinDispatch] failed to cancel sibling build exec", e);
            }
        }
    }

    /**
     * Phase 2: inject the captured dimension IPC into the top worker as broadcast instructions,
     * enrich the cascade shuffle levels, strip the already-run dimension build stages from the worker
     * children, and dispatch the rewritten DAG.
     */
    private void startPhase2(
        QueryContext ctx,
        QueryDAG rewrittenDag,
        Stage topWorker,
        List<CascadeShuffleDAGRewriter.WorkerLevel> levels,
        Map<String, byte[]> capturedByName,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        // Inject one broadcast instruction per dimension onto the top worker's plan alternatives. The
        // data-node worker handler runs the instruction chain before executing the fragment, so each
        // broadcast-<id> NamedScan in the worker fragment resolves to the registered memtable. The
        // buildSideIndex is informational (the join's NamedScan is looked up by name, not index).
        enrichWorkerWithBroadcasts(topWorker, capturedByName);

        // Reuse the cascade's per-level shuffle enrichment for every join level (including the top
        // worker that now also holds the PARTIAL aggregate — its join still consumes two shuffles).
        CascadeShuffleDispatch.enrichLevels(levels, ctx, clusterService, capabilityRegistry);

        // Strip the already-run dimension build stages from the worker's children so the scheduler
        // does not re-run them (their output is already captured + injected). Mirrors BroadcastDispatch
        // pass 2 dropping the build child. The shuffle producer children stay.
        QueryDAG dispatchDag = new QueryDAG(rewrittenDag.queryId(), stripBuildChildren(rewrittenDag.rootStage(), topWorker.getStageId()));

        LOGGER.debug(
            "[DistributedAggOverJoinDispatch] phase 2: {} dimension broadcast(s) injected into worker {}, dispatching cascade",
            capturedByName.size(),
            topWorker.getStageId()
        );
        QueryExecution exec = scheduler.execute(ctx.withDag(dispatchDag), terminal);
        if (queryExecutionSink != null) {
            queryExecutionSink.accept(exec);
        }
    }

    /** Appends a {@link BroadcastInjectionInstructionNode} per captured dimension to every plan
     *  alternative of {@code worker}. Package-private for unit testing the mutation in isolation. */
    static void enrichWorkerWithBroadcasts(Stage worker, Map<String, byte[]> capturedByName) {
        List<StagePlan> enriched = new ArrayList<>(worker.getPlanAlternatives().size());
        for (StagePlan sp : worker.getPlanAlternatives()) {
            List<InstructionNode> merged = new ArrayList<>(sp.instructions());
            for (Map.Entry<String, byte[]> e : capturedByName.entrySet()) {
                // buildSideIndex 0: informational only (NamedScan resolved by name on the data node).
                merged.add(new BroadcastInjectionInstructionNode(e.getKey(), 0, e.getValue()));
            }
            enriched.add(sp.withInstructions(merged));
        }
        worker.setPlanAlternatives(enriched);
    }

    /** Rebuilds the DAG dropping the BROADCAST_BUILD children of the worker stage (id {@code workerStageId}).
     *  Every other stage is copied with its children rebuilt so siblings are preserved. */
    private static Stage stripBuildChildren(Stage stage, int workerStageId) {
        List<Stage> children = stage.getChildStages();
        List<Stage> rebuilt = new ArrayList<>(children.size());
        boolean changed = false;
        for (Stage child : children) {
            if (stage.getStageId() == workerStageId && child.getRole() == Stage.StageRole.BROADCAST_BUILD) {
                changed = true; // drop this build child — already captured
                continue;
            }
            Stage rebuiltChild = stripBuildChildren(child, workerStageId);
            rebuilt.add(rebuiltChild);
            if (rebuiltChild != child) {
                changed = true;
            }
        }
        if (!changed) {
            return stage;
        }
        Stage copy = new Stage(
            stage.getStageId(),
            stage.getFragment(),
            rebuilt,
            stage.getExchangeInfo(),
            stage.getExchangeSinkProvider(),
            stage.getTargetResolver()
        );
        copy.setRole(stage.getRole());
        copy.setPlanAlternatives(stage.getPlanAlternatives());
        if (stage.getInstructionHandlerFactory() != null) {
            copy.setInstructionHandlerFactory(stage.getInstructionHandlerFactory());
        }
        return copy;
    }

    private List<String> resolveTargetWorkerNodeIds(int partitionCount) {
        Map<String, DiscoveryNode> dataNodes = clusterService.state().nodes().getDataNodes();
        if (dataNodes == null || dataNodes.isEmpty()) {
            return List.of();
        }
        List<String> nodeIds = new ArrayList<>(dataNodes.keySet());
        List<String> targets = new ArrayList<>(partitionCount);
        for (int p = 0; p < partitionCount; p++) {
            targets.add(nodeIds.get(p % nodeIds.size()));
        }
        return targets;
    }

    @SuppressWarnings("unchecked")
    private byte[] extractIpcBytes(ExchangeSink captureSink) throws Exception {
        Method m = captureSink.getClass().getMethod("ipcBytesFuture");
        CompletableFuture<byte[]> fut = (CompletableFuture<byte[]>) m.invoke(captureSink);
        return fut.get(EXTRACT_IPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
