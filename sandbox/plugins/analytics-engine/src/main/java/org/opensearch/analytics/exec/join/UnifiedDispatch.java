/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.QueryExecution;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.GeneralShuffleDAGRewriter;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.BroadcastInjectionInstructionNode;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.InstructionNode;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.ToLongFunction;

/**
 * Single dispatch entry point for the GENERAL post-CBO scheduler (Option B — see
 * {@code MPP-GENERAL-SCHEDULING-DESIGN.md}). Drives the DAG that {@link DistributionEnforcementPass}
 * produced, with ONE general principle that needs no per-query-shape recognition:
 *
 * <p><b>Broadcast is an INSTRUCTION, not a stage type.</b> A stage that consumes a broadcast carries a
 * {@link BroadcastInjectionInstructionNode} on its plan; the data-node handler chain registers the
 * broadcast memtable before running the fragment. Because instructions COMPOSE (the handler chain runs
 * them in order, each adding to the same native session — see
 * {@code AnalyticsSearchService.applyInstructionHandlers}), the SAME stage can be both a broadcast
 * consumer AND a shuffle producer / worker: broadcast-inject, then join, then hash-partition + ship. So
 * we never need a stage to hold two {@code StageRole}s. This dispatcher therefore:
 *
 * <ol>
 *   <li><b>Capture phase.</b> For every {@link Stage.StageRole#BROADCAST_BUILD} stage, run its subtree
 *       in isolation into an Arrow-IPC capture sink and collect the bytes keyed by
 *       {@code broadcast-<buildStageId>}.</li>
 *   <li><b>Inject + strip.</b> Append a {@code BroadcastInjectionInstructionNode} to the CONSUMER stage
 *       of each build — located by the {@link OpenSearchBroadcastScan} that references the build id, which
 *       may be a shard leaf, a shuffle producer, OR a worker — and drop the captured build children.</li>
 *   <li><b>Dispatch the broadcast-free DAG.</b> If it still distributes a join, promote its shuffle worker
 *       tiers ({@link GeneralShuffleDAGRewriter}) + enrich ({@link ShuffleEnrichment#enrichLevels}) and run;
 *       otherwise a plain {@link QueryScheduler#execute}. The injected broadcast rides on whatever stage
 *       consumes it.</li>
 * </ol>
 *
 * <p>This composes broadcast with shuffle at ANY nesting (a small build under a shuffle cascade — TPC-H
 * q3/q8/q9; a shuffle join above a broadcast — q17; a standalone broadcast — q12) because the broadcast
 * is resolved away into an instruction before the shuffle promotion ever runs.
 *
 * <p><b>Runtime filters ride the same two mechanisms.</b> A shuffle join's filter needs a value that only
 * the build side knows, so a pre-pass phase runs first — ahead of the capture phase above — summarising
 * each build side into one Bloom filter; the value then travels as an instruction exactly as a broadcast
 * does. Both are attached in the same post-promotion slot, since the promotion replaces every stage's
 * plan alternatives and would discard anything attached earlier.
 *
 * @opensearch.internal
 */
public final class UnifiedDispatch {

    private static final Logger LOGGER = LogManager.getLogger(UnifiedDispatch.class);

    /** Tripwire for the capture-sink contract: close() must settle ipcBytesFuture synchronously. */
    private static final long EXTRACT_IPC_TIMEOUT_SECONDS = 30L;

    private final QueryScheduler scheduler;
    private final ClusterService clusterService;
    private final CapabilityRegistry capabilityRegistry;
    private final boolean preferMetadataDriver;
    private final long sortMergeJoinMinRows;
    private final boolean runtimeFilterEnabled;
    private final int runtimeFilterMaxValues;
    private final int runtimeFilterMaxBloomBytes;
    private final long runtimeFilterBuildSideMaxRows;
    private final long runtimeFilterProbeSideMinScanBytes;
    private final RuntimeFilterMetrics runtimeFilterMetrics;

    public UnifiedDispatch(
        QueryScheduler scheduler,
        ClusterService clusterService,
        CapabilityRegistry capabilityRegistry,
        boolean preferMetadataDriver,
        long sortMergeJoinMinRows,
        boolean runtimeFilterEnabled,
        int runtimeFilterMaxValues,
        int runtimeFilterMaxBloomBytes,
        long runtimeFilterBuildSideMaxRows,
        long runtimeFilterProbeSideMinScanBytes,
        RuntimeFilterMetrics runtimeFilterMetrics
    ) {
        this.scheduler = scheduler;
        this.clusterService = clusterService;
        this.capabilityRegistry = capabilityRegistry;
        this.preferMetadataDriver = preferMetadataDriver;
        this.sortMergeJoinMinRows = sortMergeJoinMinRows;
        this.runtimeFilterEnabled = runtimeFilterEnabled;
        this.runtimeFilterMaxValues = runtimeFilterMaxValues;
        this.runtimeFilterMaxBloomBytes = runtimeFilterMaxBloomBytes;
        this.runtimeFilterBuildSideMaxRows = runtimeFilterBuildSideMaxRows;
        this.runtimeFilterProbeSideMinScanBytes = runtimeFilterProbeSideMinScanBytes;
        this.runtimeFilterMetrics = runtimeFilterMetrics;
    }

    /**
     * Drives the enforced DAG. When it contains any broadcast build, runs the capture phase first, then
     * dispatches the broadcast-free residual; otherwise dispatches directly.
     *
     * @param captureSinkFactory creates the backend IPC capture sink for a build stage's output rowType
     *     (the sink exposes {@code CompletableFuture<byte[]> ipcBytesFuture()} completed by {@code close()}).
     */
    public void run(
        QueryContext ctx,
        QueryDAG dag,
        Function<Stage, ExchangeSink> captureSinkFactory,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> rawTerminal
    ) {
        AtomicBoolean done = new AtomicBoolean(false);
        ActionListener<Iterable<VectorSchemaRoot>> terminal = onceOnly(done, rawTerminal);
        try {
            if (runtimeFilterEnabled && GeneralShuffleDAGRewriter.hasDistributedJoin(dag)) {
                // Planted before the shuffle promotion: that promotion re-runs the plan-side pipeline, so a
                // predicate planted now is converted for free and one planted later never reaches a plan.
                // The distributed-join precondition is what guarantees the promotion runs at all.
                List<ShuffleRuntimeFilters.Descriptor> planned = ShuffleRuntimeFilters.plan(
                    dag,
                    runtimeFilterMaxBloomBytes,
                    runtimeFilterBuildSideMaxRows,
                    runtimeFilterProbeSideMinScanBytes
                );
                runtimeFilterMetrics.record(RuntimeFilterMetrics.Counter.SHUFFLE_PLANNED, planned.size());
                ShuffleRuntimeFilters.Planted planted = ShuffleRuntimeFilters.plantProbePredicates(dag, planned);
                runtimeFilterMetrics.record(RuntimeFilterMetrics.Counter.SHUFFLE_PLANTED, planted.descriptors().size());
                if (!planted.descriptors().isEmpty()) {
                    prePassThenDispatch(ctx, planted, captureSinkFactory, queryExecutionSink, terminal);
                    return;
                }
            }
            dispatch(ctx, dag, captureSinkFactory, queryExecutionSink, terminal, NO_PAYLOADS);
        } catch (Exception e) {
            terminal.onFailure(e);
        }
    }

    /** No runtime-filter payloads to attach — the shape of every query that has no shuffle filter. */
    private static final Consumer<QueryDAG> NO_PAYLOADS = rewrittenDag -> {};

    /**
     * Dispatches the query proper: broadcast capture first when it has any build, else straight through.
     *
     * @param attachPayloads applied to the rewritten DAG in the same {@code postRewrite} slot the broadcast
     *     injection uses, to attach whatever the pre-pass computed
     */
    private void dispatch(
        QueryContext ctx,
        QueryDAG dag,
        Function<Stage, ExchangeSink> captureSinkFactory,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal,
        Consumer<QueryDAG> attachPayloads
    ) {
        List<List<Stage>> waves = collectBuildWaves(dag.rootStage());
        if (waves.isEmpty()) {
            dispatchBroadcastFree(ctx, dag, rewrittenDag -> {
                attachPayloads.accept(rewrittenDag);
                return rewrittenDag;
            }, queryExecutionSink, terminal);
            return;
        }
        captureThenDispatch(ctx, dag, waves, captureSinkFactory, queryExecutionSink, terminal, attachPayloads);
    }

    /**
     * Runs one pre-pass per planted shuffle filter, then dispatches the query with whatever payloads those
     * pre-passes produced.
     *
     * <p>Each pre-pass is a standalone mini-DAG over the build producer's own fragment, so it needs its own
     * plan-side pipeline: it was built by hand after {@code DefaultPlanExecutor} ran that pipeline, and an
     * unconverted stage has no plan to send to a data node.
     *
     * <p>Every failure mode here falls through to dispatching the query <em>without</em> that payload: a
     * pre-pass that fails, is cancelled, or yields nothing leaves its predicate unsatisfied, and an
     * unsatisfied predicate keeps every row. So a broken pre-pass costs a scan and the optimization, never
     * the result. Task cancellation is the one exception — there the query is going away regardless.
     */
    private void prePassThenDispatch(
        QueryContext ctx,
        ShuffleRuntimeFilters.Planted planted,
        Function<Stage, ExchangeSink> captureSinkFactory,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        QueryDAG dag = planted.dag();
        Map<Integer, Stage> stagesById = new LinkedHashMap<>();
        indexStages(dag.rootStage(), stagesById);

        Map<Integer, ShuffleRuntimeFilters.Descriptor> byPrePassStageId = new LinkedHashMap<>();
        List<Stage> prePassStages = new ArrayList<>();
        for (ShuffleRuntimeFilters.Descriptor descriptor : planted.descriptors()) {
            Stage buildStage = stagesById.get(descriptor.buildStageId());
            if (buildStage == null) {
                continue;
            }
            QueryDAG prePass = ShuffleRuntimeFilters.prePassDag(dag.queryId() + "-rf" + descriptor.filterId(), buildStage, descriptor);
            if (prePass == null) {
                continue;
            }
            PlanForker.forkAll(prePass, capabilityRegistry);
            BackendPlanAdapter.adaptAll(prePass, capabilityRegistry);
            PlanAlternativeSelector.selectAll(prePass, capabilityRegistry, preferMetadataDriver);
            FragmentConversionDriver.convertAll(prePass, capabilityRegistry);
            prePassStages.add(prePass.rootStage());
            byPrePassStageId.put(prePass.rootStage().getStageId(), descriptor);
        }
        if (prePassStages.isEmpty()) {
            dispatch(ctx, dag, captureSinkFactory, queryExecutionSink, terminal, NO_PAYLOADS);
            return;
        }

        runtimeFilterMetrics.record(RuntimeFilterMetrics.Counter.PRE_PASS_RUN, prePassStages.size());
        final long prePassStartNanos = System.nanoTime();
        Map<Integer, byte[]> payloadsByFilterId = new LinkedHashMap<>();
        // One merging sink per pre-pass, unioning as contributions arrive. An IPC capture sink would have to
        // reserve `bloomBytes × shards` up front, which exhausted the native pool on multi-filter queries.
        Map<Integer, RuntimeFilterMergeSink> sinksByStageId = new LinkedHashMap<>();
        for (Map.Entry<Integer, ShuffleRuntimeFilters.Descriptor> e : byPrePassStageId.entrySet()) {
            sinksByStageId.put(e.getKey(), new RuntimeFilterMergeSink(e.getValue().filterId()));
        }
        capturePrePasses(ctx, prePassStages, stage -> sinksByStageId.get(stage.getStageId()), prePassStageId -> {
            ShuffleRuntimeFilters.Descriptor descriptor = byPrePassStageId.get(prePassStageId);
            RuntimeFilterMergeSink sink = sinksByStageId.get(prePassStageId);
            byte[] merged = sink == null ? null : sink.mergedBitset();
            if (descriptor != null && merged != null) {
                runtimeFilterMetrics.increment(RuntimeFilterMetrics.Counter.PRE_PASS_WITH_PAYLOAD);
                runtimeFilterMetrics.record(RuntimeFilterMetrics.Counter.PAYLOAD_BYTES, merged.length);
                synchronized (payloadsByFilterId) {
                    payloadsByFilterId.put(descriptor.filterId(), merged);
                }
            }
        }, () -> {
            // Per phase, not per pre-pass: they run concurrently, so the phase's elapsed time is what any
            // measured win pays, not the sum of its parts.
            runtimeFilterMetrics.record(RuntimeFilterMetrics.Counter.PRE_PASS_NANOS, System.nanoTime() - prePassStartNanos);
            try {
                LOGGER.debug(
                    "[runtime-filter] {} of {} pre-pass(es) produced a payload; dispatching",
                    payloadsByFilterId.size(),
                    prePassStages.size()
                );
                dispatch(ctx, dag, captureSinkFactory, queryExecutionSink, terminal, rewrittenDag -> {
                    int attached = ShuffleRuntimeFilterPayload.attach(rewrittenDag.rootStage(), payloadsByFilterId);
                    runtimeFilterMetrics.record(RuntimeFilterMetrics.Counter.PAYLOAD_ATTACHED, attached);
                    LOGGER.debug("[runtime-filter] attached {} shuffle filter payload(s)", attached);
                });
            } catch (Exception e) {
                terminal.onFailure(e);
            }
        }, terminal);
    }

    /**
     * Runs each pre-pass subtree in isolation into its own sink, reporting every settled stage to
     * {@code onSettled} and calling {@code onAllSettled} once, after the last one.
     *
     * <p>Whether a pre-pass produced anything is read from its sink by the caller rather than passed here:
     * the sink accumulates the union as contributions arrive, so a settled stage is the signal and the sink
     * is the value. That makes the terminal state load-bearing — a stage that did not succeed has its sink
     * invalidated here, before the caller can read a union that is missing a shard.</p>
     *
     * <p>Unlike the broadcast capture phase, a failure is not fatal and siblings are not cancelled: a
     * missing payload is a missing optimization. What must not be swallowed is task cancellation, which
     * fails the query through {@code terminal} as everywhere else.
     */
    private void capturePrePasses(
        QueryContext ctx,
        List<Stage> prePassStages,
        Function<Stage, ExchangeSink> captureSinkFactory,
        IntConsumer onSettled,
        Runnable onAllSettled,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        StageExecutionBuilder builder = scheduler.getStageExecutionBuilder();
        AtomicInteger remaining = new AtomicInteger(prePassStages.size());
        List<StageExecution> roots = new ArrayList<>(prePassStages.size());
        List<StageExecution> allLeaves = new ArrayList<>();

        for (Stage prePassStage : prePassStages) {
            final int prePassStageId = prePassStage.getStageId();
            ExchangeSink captureSink = captureSinkFactory.apply(prePassStage);
            StageExecutionBuilder.SubGraph graph = builder.buildSubGraphWithSink(prePassStage, captureSink, ctx, scheduler::scheduleStage);
            roots.add(graph.root());
            allLeaves.addAll(graph.leaves());
            graph.root().addStateListener((from, to) -> {
                switch (to) {
                    case SUCCEEDED -> {
                        // Nothing to extract: the sink accumulates the union and the caller reads it
                        // directly.
                        try {
                            captureSink.close();
                        } catch (Throwable t) {
                            LOGGER.warn(new ParameterizedMessage("[runtime-filter] pre-pass {} sink close failed", prePassStageId), t);
                        }
                    }
                    case FAILED, CANCELLED -> {
                        // Abandon the accumulator BEFORE anyone can read it: a pre-pass that got part way
                        // has already unioned the shards that did report, and a union missing a shard's keys
                        // makes the probe reject rows that belong in the result. The sink's per-contribution
                        // checks cannot catch it — every contribution it received was well formed.
                        if (captureSink instanceof RuntimeFilterMergeSink mergeSink) {
                            mergeSink.invalidate();
                        }
                        try {
                            captureSink.close();
                        } catch (Throwable ignore) {
                            // The stage already failed; a close failure adds nothing.
                        }
                        LOGGER.debug("[runtime-filter] pre-pass {} ended {}; continuing without its payload", prePassStageId, to);
                    }
                    default -> {
                        return;
                    }
                }
                onSettled.accept(prePassStageId);
                if (remaining.decrementAndGet() == 0) {
                    AnalyticsQueryTask task = ctx.parentTask();
                    if (task != null && task.isCancelled()) {
                        String reason = task.getReasonCancelled() != null ? task.getReasonCancelled() : "unknown";
                        terminal.onFailure(new TaskCancelledException("query cancelled during runtime-filter pre-pass: " + reason));
                        return;
                    }
                    onAllSettled.run();
                }
            });
        }

        // Cancel wiring goes last, as in the capture phase: the callback replays synchronously when the task
        // is already cancelled. The next phase replaces it.
        AnalyticsQueryTask parentTask = ctx.parentTask();
        if (parentTask != null) {
            parentTask.setOnCancelCallback(() -> {
                String reason = parentTask.getReasonCancelled() != null ? parentTask.getReasonCancelled() : "unknown";
                for (StageExecution root : roots) {
                    try {
                        root.cancel("task cancelled: " + reason);
                    } catch (Exception e) {
                        LOGGER.warn("[runtime-filter] failed to cancel pre-pass exec", e);
                    }
                }
            });
            if (parentTask.isCancelled()) {
                LOGGER.debug("[runtime-filter] task already cancelled before the pre-pass started; not scheduling");
                return;
            }
        }

        for (StageExecution leaf : allLeaves) {
            scheduler.scheduleStage(leaf);
        }
    }

    private static void indexStages(Stage stage, Map<Integer, Stage> out) {
        out.put(stage.getStageId(), stage);
        for (Stage child : stage.getChildStages()) {
            indexStages(child, out);
        }
    }

    /**
     * Dispatches a DAG that has NO broadcast build (the common no-broadcast case): shuffle promotion if it
     * distributes a join, else plain. {@code postRewrite} is an optional hook applied to the rewritten DAG
     * AFTER the shuffle promotion's {@code forkAll → convertAll} pipeline but BEFORE {@code scheduler.execute}
     * — the broadcast path uses it to inject broadcast instructions into the (now post-fork) consumer stages
     * so they survive the alternative re-expansion. Without a broadcast it is identity.
     */
    private void dispatchBroadcastFree(
        QueryContext ctx,
        QueryDAG dag,
        Function<QueryDAG, QueryDAG> postRewrite,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        if (GeneralShuffleDAGRewriter.hasDistributedJoin(dag)) {
            // Promote shuffle joins to worker tiers (forkAll → adaptAll → selectAll → convertAll), THEN apply
            // postRewrite (broadcast injection survives because it runs after the alternative re-expansion),
            // THEN enrich the per-level shuffle instructions and dispatch.
            GeneralShuffleDAGRewriter.Rewritten rewritten = GeneralShuffleDAGRewriter.rewrite(
                dag,
                capabilityRegistry,
                preferMetadataDriver,
                (levelIndex, partitionCount) -> resolveTargetWorkerNodeIds(partitionCount)
            );
            QueryDAG finalDag = postRewrite.apply(rewritten.dag());
            ShuffleEnrichment.enrichLevels(rewritten.levels(), ctx, clusterService, capabilityRegistry, sortMergeJoinMinRows);
            QueryExecution exec = scheduler.execute(ctx.withDag(finalDag), terminal);
            if (queryExecutionSink != null) {
                queryExecutionSink.accept(exec);
            }
        } else {
            QueryDAG finalDag = postRewrite.apply(dag);
            QueryExecution exec = scheduler.execute(ctx.withDag(finalDag), terminal);
            if (queryExecutionSink != null) {
                queryExecutionSink.accept(exec);
            }
        }
    }

    /**
     * Capture phase, run as WAVES ordered innermost-first, then inject + strip + dispatch the
     * broadcast-free residual.
     *
     * <p><b>Why waves.</b> A broadcast build can itself be a broadcast probe — nested broadcast, e.g. a
     * customer scan that is both build-for-one-join and probe-of-another. Such a build cannot be built until
     * its OWN build has been captured and stripped out of its subtree, because otherwise
     * {@code buildSubGraphWithSink} walks into that un-stripped child and asks a shard fragment to be its
     * {@code DataConsumer}, which it is not. Capturing every build in one flat pass (the previous shape)
     * therefore failed outright on any nested-broadcast plan. Each wave depends only on shallower-depth
     * results, so within a wave the builds still run concurrently.
     *
     * <p>The residual dispatch happens once, after the LAST (shallowest) wave.
     */
    private void captureThenDispatch(
        QueryContext ctx,
        QueryDAG dag,
        List<List<Stage>> waves,
        Function<Stage, ExchangeSink> captureSinkFactory,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal,
        Consumer<QueryDAG> attachPayloads
    ) {
        // Accumulates across waves: a wave's builds are prepared against everything captured before it.
        Map<Integer, byte[]> capturedByBuildId = new LinkedHashMap<>();
        // The cancel callback is installed once, by the first wave, but must cancel whichever wave is in
        // flight — hence the shared reference rather than a capture of one wave's list.
        AtomicReference<List<StageExecution>> activeRoots = new AtomicReference<>(List.of());
        AtomicBoolean cancelCallbackInstalled = new AtomicBoolean(false);
        captureWave(
            ctx,
            dag,
            waves,
            0,
            capturedByBuildId,
            activeRoots,
            cancelCallbackInstalled,
            captureSinkFactory,
            queryExecutionSink,
            terminal,
            attachPayloads
        );
    }

    /**
     * Builds and schedules every build in wave {@code waveIndex} concurrently. On wave completion, either
     * advances to the next (shallower) wave or dispatches the residual.
     */
    private void captureWave(
        QueryContext ctx,
        QueryDAG dag,
        List<List<Stage>> waves,
        int waveIndex,
        Map<Integer, byte[]> capturedByBuildId,
        AtomicReference<List<StageExecution>> activeRoots,
        AtomicBoolean cancelCallbackInstalled,
        Function<Stage, ExchangeSink> captureSinkFactory,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal,
        Consumer<QueryDAG> attachPayloads
    ) {
        StageExecutionBuilder builder = scheduler.getStageExecutionBuilder();
        List<Stage> builds = waves.get(waveIndex);
        AtomicInteger remaining = new AtomicInteger(builds.size());
        List<StageExecution> buildRoots = new ArrayList<>(builds.size());
        List<StageExecution> allLeaves = new ArrayList<>();

        for (Stage rawBuildStage : builds) {
            // Prepare against earlier waves: drop the build children already captured, then hand this stage
            // their payloads so its own OpenSearchBroadcastScan placeholders resolve. Both are no-ops in the
            // common single-level case, where capturedByBuildId is still empty.
            Stage buildStage = stripBuildChildren(rawBuildStage, capturedByBuildId);
            injectBroadcastsInPlace(buildStage, capturedByBuildId);
            final int buildId = buildStage.getStageId();
            ExchangeSink captureSink = captureSinkFactory.apply(buildStage);
            StageExecutionBuilder.SubGraph buildGraph = builder.buildSubGraphWithSink(
                buildStage,
                captureSink,
                ctx,
                scheduler::scheduleStage
            );
            StageExecution buildExec = buildGraph.root();
            buildRoots.add(buildExec);
            allLeaves.addAll(buildGraph.leaves());
            // Listener BEFORE the cancel callback (setOnCancelCallback replays synchronously when already
            // cancelled — a later listener would miss the CANCELLED transition; the BroadcastDispatch /
            // DistributedAggOverJoinDispatch ordering contract).
            buildExec.addStateListener((from, to) -> {
                switch (to) {
                    case SUCCEEDED -> {
                        byte[] ipc;
                        try {
                            captureSink.close();
                            ipc = extractIpcBytes(captureSink);
                        } catch (Throwable t) {
                            LOGGER.warn(
                                new ParameterizedMessage("[UnifiedDispatch] broadcast build capture failed for build {}", buildId),
                                t
                            );
                            cancelOtherBuilds(buildRoots, buildExec, "sibling broadcast build capture failed");
                            terminal.onFailure(new RuntimeException("UnifiedDispatch: broadcast build capture failed", t));
                            return;
                        }
                        synchronized (capturedByBuildId) {
                            capturedByBuildId.put(buildId, ipc);
                        }
                        LOGGER.debug(
                            "[UnifiedDispatch] captured broadcast build wave={} build={} ipcBytes={}",
                            waveIndex,
                            buildId,
                            ipc == null ? -1 : ipc.length
                        );
                        if (remaining.decrementAndGet() == 0) {
                            if (ctx.parentTask() != null && ctx.parentTask().isCancelled()) {
                                String reason = ctx.parentTask().getReasonCancelled() != null
                                    ? ctx.parentTask().getReasonCancelled()
                                    : "unknown";
                                terminal.onFailure(new TaskCancelledException("query cancelled during broadcast builds: " + reason));
                                return;
                            }
                            try {
                                if (waveIndex + 1 < waves.size()) {
                                    captureWave(
                                        ctx,
                                        dag,
                                        waves,
                                        waveIndex + 1,
                                        capturedByBuildId,
                                        activeRoots,
                                        cancelCallbackInstalled,
                                        captureSinkFactory,
                                        queryExecutionSink,
                                        terminal,
                                        attachPayloads
                                    );
                                } else {
                                    injectStripAndDispatch(ctx, dag, capturedByBuildId, queryExecutionSink, terminal, attachPayloads);
                                }
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
                        cancelOtherBuilds(buildRoots, buildExec, "sibling broadcast build failed");
                        terminal.onFailure(cause != null ? cause : new RuntimeException("broadcast build stage " + buildId + " FAILED"));
                    }
                    case CANCELLED -> {
                        try {
                            captureSink.close();
                        } catch (Throwable ignore) {
                            // primary cancel surfaced below
                        }
                        cancelOtherBuilds(buildRoots, buildExec, "sibling broadcast build cancelled");
                        terminal.onFailure(new RuntimeException("broadcast build stage " + buildId + " CANCELLED"));
                    }
                    default -> {
                        // CREATED / RUNNING — no action.
                    }
                }
            });
        }

        // Publish this wave's roots AFTER every listener is installed, so the cancel callback cancels the
        // wave that is actually in flight.
        activeRoots.set(buildRoots);
        // Install the cancel callback ONCE, and only now that a wave has roots to cancel. Ordering is
        // load-bearing: setOnCancelCallback REPLAYS synchronously when the task is already cancelled, and it
        // is that replay — cancelling each root, whose CANCELLED listener fails the terminal — that reports
        // the cancellation. Installing it before any roots exist makes the replay cancel nothing, no listener
        // fires, and the query hangs. UnifiedDispatchTests#testRunFailsTerminalWhenTaskCancelledBeforeRun
        // guards exactly that.
        if (cancelCallbackInstalled.compareAndSet(false, true)) {
            installCaptureCancelCallback(ctx, activeRoots);
        }
        AnalyticsQueryTask parentTask = ctx.parentTask();
        if (parentTask != null && parentTask.isCancelled()) {
            LOGGER.debug("[UnifiedDispatch] task cancelled before wave {} was scheduled; cancelling its roots", waveIndex);
            // Cancel THIS wave's roots explicitly rather than just returning. The cancellation may have landed
            // in the window between the previous wave succeeding and this wave publishing its roots, in which
            // case the one-shot cancel callback fired against the PREVIOUS wave's roots — all terminal by then,
            // and cancel() no-ops on a terminal state, so no listener ran. Returning here without cancelling
            // would leave nothing to complete the terminal and the query would hang. Driving our own roots to
            // CANCELLED routes through the same CANCELLED listener every other cancel path uses, and `terminal`
            // is once-only, so the query is failed exactly once.
            for (StageExecution buildExec : buildRoots) {
                try {
                    buildExec.cancel("task cancelled before wave " + waveIndex + " was scheduled");
                } catch (Exception e) {
                    LOGGER.debug(new ParameterizedMessage("[UnifiedDispatch] cancel failed for build {}", buildExec.getStageId()), e);
                }
            }
            return;
        }

        for (StageExecution leaf : allLeaves) {
            scheduler.scheduleStage(leaf);
        }
    }

    /**
     * Installs the capture-phase cancel callback ONCE, from the FIRST wave and only after that wave has
     * published its roots to {@code activeRoots} (see the caller — the replay ordering is load-bearing). It
     * bypasses the normal {@code QueryScheduler.execute} cancel path (the capture schedules build leaves
     * directly), and reads {@code activeRoots} rather than closing over one wave's list so a cancel arriving
     * during a later wave still cancels live executions. Phase-2's {@code execute} replaces this callback with
     * its own walker-level cancel.
     */
    private static void installCaptureCancelCallback(QueryContext ctx, AtomicReference<List<StageExecution>> activeRoots) {
        AnalyticsQueryTask parentTask = ctx.parentTask();
        if (parentTask == null) {
            return;
        }
        parentTask.setOnCancelCallback(() -> {
            String reason = parentTask.getReasonCancelled() != null ? parentTask.getReasonCancelled() : "unknown";
            LOGGER.debug("[UnifiedDispatch] capture phase cancel requested, reason={}", reason);
            for (StageExecution buildExec : activeRoots.get()) {
                try {
                    buildExec.cancel("task cancelled: " + reason);
                } catch (Exception e) {
                    LOGGER.warn("[UnifiedDispatch] failed to cancel build exec", e);
                }
            }
        });
    }

    /**
     * Strip the captured build children, then dispatch the broadcast-free DAG with broadcast injection
     * deferred to a {@code postRewrite} hook. ORDER MATTERS: the shuffle promotion runs
     * {@code forkAll → convertAll} which REPLACES every stage's plan alternatives, so the broadcast
     * instruction must be injected AFTER that pipeline (the hook), not before — otherwise it is wiped.
     * The build children are stripped BEFORE the rewrite so the rewriter never tries to promote/wire an
     * already-captured build.
     */
    private void injectStripAndDispatch(
        QueryContext ctx,
        QueryDAG dag,
        Map<Integer, byte[]> capturedByBuildId,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal,
        Consumer<QueryDAG> attachPayloads
    ) {
        Stage strippedRoot = stripBuildChildren(dag.rootStage(), capturedByBuildId);
        QueryDAG strippedDag = new QueryDAG(dag.queryId(), strippedRoot);
        LOGGER.debug(
            "[UnifiedDispatch] stripped {} captured build(s); dispatching broadcast-free DAG with deferred injection",
            capturedByBuildId.size()
        );
        // postRewrite: inject the captured broadcasts into the (post-fork) consumer stages IN PLACE. In-place
        // mutation (not a rebuild) is required so it composes with ShuffleEnrichment.enrichLevels, which
        // also appends instructions in place to the same worker stages — a rebuild here would either discard
        // the enrich or be discarded by it. Returns the same DAG.
        dispatchBroadcastFree(ctx, strippedDag, rewrittenDag -> {
            injectBroadcastsInPlace(rewrittenDag.rootStage(), capturedByBuildId);
            // Same hook, same reason: anything attached before the shuffle promotion is wiped when it
            // replaces every stage's plan alternatives. The build side is already captured here, so the key
            // set costs no extra scan.
            if (runtimeFilterEnabled) {
                int canMatchAttached = BroadcastRuntimeFilters.attach(
                    rewrittenDag.rootStage(),
                    capturedByBuildId,
                    ctx.bufferAllocator(),
                    runtimeFilterMaxValues
                );
                // Recorded, not discarded: this is the only signal that the broadcast half fired, and an
                // unrecorded counter is indistinguishable from a real failure.
                runtimeFilterMetrics.record(RuntimeFilterMetrics.Counter.BROADCAST_CAN_MATCH_ATTACHED, canMatchAttached);
            }
            // The shuffle family's payload lands in the same slot, after the broadcast work, so a query
            // using both gets both instructions.
            attachPayloads.accept(rewrittenDag);
            return rewrittenDag;
        }, queryExecutionSink, terminal);
    }

    /** Rebuilds the DAG dropping every {@code BROADCAST_BUILD} child whose id was captured (its output is
     *  already injected). Other stages are copied with children rebuilt so siblings are preserved. */
    private static Stage stripBuildChildren(Stage stage, Map<Integer, byte[]> capturedByBuildId) {
        List<Stage> rebuiltChildren = new ArrayList<>(stage.getChildStages().size());
        boolean changed = false;
        for (Stage child : stage.getChildStages()) {
            if (child.getRole() == Stage.StageRole.BROADCAST_BUILD && capturedByBuildId.containsKey(child.getStageId())) {
                changed = true;
                continue;
            }
            Stage rebuilt = stripBuildChildren(child, capturedByBuildId);
            rebuiltChildren.add(rebuilt);
            if (rebuilt != child) {
                changed = true;
            }
        }
        if (!changed) {
            return stage;
        }
        return copyStage(stage, rebuiltChildren, stage.getPlanAlternatives());
    }

    /**
     * Appends a {@link BroadcastInjectionInstructionNode} IN PLACE to the plan alternatives of every stage
     * whose fragment contains an {@link OpenSearchBroadcastScan} for a captured build. The consumer may be a
     * shard leaf, a shuffle producer, or a worker — the instruction composes with whatever else the stage
     * does. Runs BEFORE {@code ShuffleEnrichment.enrichLevels}, so the handler chain ends up as
     * {@code ShardScan → BroadcastInjection → ShuffleProducer} (BroadcastInjectionHandler requires running
     * after the shard scan creates the session and before the producer ships partitions). In-place so the
     * subsequent shuffle enrichment sees the same stage objects.
     */
    private static void injectBroadcastsInPlace(Stage stage, Map<Integer, byte[]> capturedByBuildId) {
        if (stage.getFragment() != null) {
            List<OpenSearchBroadcastScan> scans = RelNodeUtils.findNodes(stage.getFragment(), OpenSearchBroadcastScan.class);
            List<Map.Entry<Integer, byte[]>> toInject = new ArrayList<>();
            for (OpenSearchBroadcastScan scan : scans) {
                byte[] ipc = capturedByBuildId.get(scan.getBuildStageId());
                if (ipc != null) {
                    toInject.add(Map.entry(scan.getBuildStageId(), ipc));
                }
            }
            if (!toInject.isEmpty()) {
                List<StagePlan> enriched = new ArrayList<>(stage.getPlanAlternatives().size());
                for (StagePlan sp : stage.getPlanAlternatives()) {
                    List<InstructionNode> merged = new ArrayList<>(sp.instructions());
                    for (Map.Entry<Integer, byte[]> e : toInject) {
                        // buildSideIndex 0: informational only — the NamedScan resolves by name on the data node.
                        merged.add(new BroadcastInjectionInstructionNode("broadcast-" + e.getKey(), 0, e.getValue()));
                    }
                    enriched.add(sp.withInstructions(merged));
                }
                stage.setPlanAlternatives(enriched);
            }
        }
        for (Stage child : stage.getChildStages()) {
            injectBroadcastsInPlace(child, capturedByBuildId);
        }
    }

    /** Copies a stage with new children, preserving role / exchange / resolver / factory / plan alternatives. */
    private static Stage copyStage(Stage stage, List<Stage> children, List<StagePlan> planAlternatives) {
        return copyStage(stage, stage.getFragment(), children, planAlternatives);
    }

    /**
     * Copies a stage with a new fragment as well. Package-private so {@code ShuffleRuntimeFilters} can
     * plant a predicate into a producer's fragment without duplicating the field-by-field copy — a
     * missed field here silently loses a role or a resolver.
     */
    static Stage copyStage(Stage stage, RelNode fragment, List<Stage> children, List<StagePlan> planAlternatives) {
        Stage copy = new Stage(
            stage.getStageId(),
            fragment,
            children,
            stage.getExchangeInfo(),
            stage.getExchangeSinkProvider(),
            stage.getTargetResolver()
        );
        copy.setRole(stage.getRole());
        copy.setPlanAlternatives(planAlternatives);
        if (stage.getInstructionHandlerFactory() != null) {
            copy.setInstructionHandlerFactory(stage.getInstructionHandlerFactory());
        }
        return copy;
    }

    /** Round-robins one target worker node per partition across the cluster's data nodes (shuffle promotion). */
    private List<String> resolveTargetWorkerNodeIds(int partitionCount) {
        Map<String, org.opensearch.cluster.node.DiscoveryNode> dataNodes = clusterService.state().nodes().getDataNodes();
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

    /**
     * Groups every {@link Stage.StageRole#BROADCAST_BUILD} stage in the DAG into capture waves, ordered
     * INNERMOST-FIRST: wave 0 holds the most deeply nested builds, the last wave the shallowest.
     *
     * <p>A build's depth is the number of BROADCAST_BUILD stages strictly between it and the root, so a build
     * that is itself a broadcast probe lands in a strictly earlier wave than the build it feeds. That is the
     * ordering {@link #captureWave} needs: a build's own builds must already be captured (and therefore
     * strippable) before it can be built. Returns an empty list when the DAG has no broadcast at all.
     */
    private static List<List<Stage>> collectBuildWaves(Stage root) {
        Map<Integer, List<Stage>> byDepth = new LinkedHashMap<>();
        collectBuildsByDepth(root, 0, byDepth);
        if (byDepth.isEmpty()) {
            return List.of();
        }
        int maxDepth = byDepth.keySet().stream().max(Integer::compare).orElse(0);
        List<List<Stage>> waves = new ArrayList<>();
        for (int depth = maxDepth; depth >= 0; depth--) {
            List<Stage> wave = byDepth.get(depth);
            if (wave != null && !wave.isEmpty()) {
                waves.add(wave);
            }
        }
        return waves;
    }

    private static void collectBuildsByDepth(Stage stage, int depth, Map<Integer, List<Stage>> byDepth) {
        int childDepth = depth;
        if (stage.getRole() == Stage.StageRole.BROADCAST_BUILD) {
            byDepth.computeIfAbsent(depth, d -> new ArrayList<>()).add(stage);
            childDepth = depth + 1;
        }
        for (Stage child : stage.getChildStages()) {
            collectBuildsByDepth(child, childDepth, byDepth);
        }
    }

    private static ActionListener<Iterable<VectorSchemaRoot>> onceOnly(AtomicBoolean done, ActionListener<Iterable<VectorSchemaRoot>> raw) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Iterable<VectorSchemaRoot> result) {
                if (done.compareAndSet(false, true)) {
                    raw.onResponse(result);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (done.compareAndSet(false, true)) {
                    raw.onFailure(e);
                }
            }
        };
    }

    private static void cancelOtherBuilds(List<StageExecution> buildRoots, StageExecution self, String reason) {
        for (StageExecution other : buildRoots) {
            if (other == self) {
                continue;
            }
            try {
                other.cancel(reason);
            } catch (Exception e) {
                LOGGER.warn("[UnifiedDispatch] failed to cancel sibling build exec", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private byte[] extractIpcBytes(ExchangeSink captureSink) throws Exception {
        Method m = captureSink.getClass().getMethod("ipcBytesFuture");
        CompletableFuture<byte[]> fut = (CompletableFuture<byte[]>) m.invoke(captureSink);
        return fut.get(EXTRACT_IPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Resolves the IPC capture-sink factory for this query (the reduce-capable backend's broadcast capture
     * sink, sized by {@code analytics.mpp.broadcast.max_bytes}). Exposed so {@code DefaultPlanExecutor} can
     * build it once and hand it in — identical to the legacy broadcast path's factory.
     */
    public static Function<Stage, ExchangeSink> captureSinkFactory(
        QueryContext ctx,
        QueryDAG dag,
        CapabilityRegistry capabilityRegistry,
        ClusterService clusterService
    ) {
        final long broadcastMaxBytes = clusterService.getClusterSettings().get(AnalyticsSettings.BROADCAST_MAX_BYTES).getBytes();
        return captureSinkFactory(ctx, dag, capabilityRegistry, stage -> broadcastMaxBytes);
    }

    /** As above, with the per-stage byte budget supplied by the caller. */
    private static Function<Stage, ExchangeSink> captureSinkFactory(
        QueryContext ctx,
        QueryDAG dag,
        CapabilityRegistry capabilityRegistry,
        ToLongFunction<Stage> maxBytes
    ) {
        Stage root = dag.rootStage();
        List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(
            capabilityRegistry,
            ((OpenSearchRelNode) root.getFragment()).getViableBackends()
        );
        if (reduceViable.isEmpty()) {
            throw new IllegalStateException("No reduce-capable backend for broadcast capture sink (general scheduler)");
        }
        final String captureBackendId = reduceViable.get(0);
        return stage -> capabilityRegistry.getBackend(captureBackendId)
            .getExchangeSinkProvider()
            .createBroadcastCaptureSink(ctx.bufferAllocator(), stage.getFragment().getRowType(), maxBytes.applyAsLong(stage));
    }
}
