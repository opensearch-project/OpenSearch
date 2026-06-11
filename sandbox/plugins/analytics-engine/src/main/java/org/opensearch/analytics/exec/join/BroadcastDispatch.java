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
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.task.AnalyticsQueryTask;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan;
import org.opensearch.analytics.spi.BroadcastInjectionInstructionNode;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskCancelledException;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Two-pass orchestrator for broadcast-join execution.
 *
 * <p>Drives the broadcast-shape {@link QueryDAG} that {@code DAGBuilder} produced by cutting
 * at an {@link org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange} (selected by
 * Volcano CBO from the broadcast split rule's alternatives). Runs two sequential phases against
 * the shared {@link StageExecutionBuilder} and {@link QueryScheduler}:
 *
 * <ol>
 *   <li><b>Pass 1 — build-only.</b> Runs the {@link Stage.StageRole#BROADCAST_BUILD} stage in
 *       isolation, capturing its Arrow output into an Arrow-IPC byte buffer via a caller-supplied
 *       {@link ExchangeSink} factory (typically the datafusion {@code BroadcastCaptureSink}).
 *       On {@code SUCCEEDED}, proceeds to pass 2; on {@code FAILED}/{@code CANCELLED}, fails the
 *       terminal listener and never starts pass 2.</li>
 *   <li><b>Pass 2 — probe + root.</b> Builds a derived {@link QueryDAG} that drops the build
 *       stage (its output is already captured) and prepends a {@link BroadcastInjectionInstructionNode}
 *       carrying the IPC bytes to every {@link StagePlan} on the probe stage. Dispatches via
 *       the normal {@link QueryScheduler#execute} path — each probe data node's handler chain
 *       runs {@code ShardScanInstructionHandler} first (creating a native session), then
 *       {@code BroadcastInjectionHandler} (registering the memtable), then the engine executes
 *       {@code Join(ShardScan, NamedScan("broadcast-&lt;buildStageId&gt;"))}.</li>
 * </ol>
 *
 * <p>Cancellation: the query's {@code AnalyticsQueryTask.onCancelCallback} is already wired by
 * {@link QueryScheduler} for pass 2. For pass 1 the orchestrator wires a listener on the build
 * stage execution so the terminal {@link ActionListener} fails promptly if cancellation races
 * with build completion — pass 2 then refuses to start.
 *
 * @opensearch.internal
 */
public final class BroadcastDispatch {

    private static final Logger LOGGER = LogManager.getLogger(BroadcastDispatch.class);

    private final StageExecutionBuilder stageExecutionBuilder;
    private final QueryScheduler scheduler;

    public BroadcastDispatch(StageExecutionBuilder stageExecutionBuilder, QueryScheduler scheduler) {
        this.stageExecutionBuilder = stageExecutionBuilder;
        this.scheduler = scheduler;
    }

    /**
     * Drives the broadcast-shape DAG through pass 1 + pass 2.
     *
     * @param ctx the query context whose DAG is {@code dag} (pass 2 will use a derived
     *     context whose DAG drops the build stage).
     * @param dag the broadcast-shape DAG cut by {@code DAGBuilder} from a CBO output that
     *     contains an {@link org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange}.
     * @param buildStage the build stage. Role must be {@link Stage.StageRole#BROADCAST_BUILD}.
     * @param probeStage the probe stage. Role must be {@link Stage.StageRole#BROADCAST_PROBE}.
     * @param rootStage the root stage of the DAG.
     * @param captureSinkFactory creates the backend-specific IPC capture sink for a given build
     *     stage (its argument), built for that build's output rowType. The factory returns an
     *     {@link ExchangeSink} that additionally exposes a {@code CompletableFuture&lt;byte[]&gt;}
     *     via {@code ipcBytesFuture()}; the dispatcher retrieves IPC bytes via reflection on that
     *     method to keep analytics-engine from depending on the DataFusion backend directly. Called
     *     once per broadcast build (multi-broadcast queries resolve N builds, each with its own sink).
     * @param queryExecutionSink optional callback invoked with the pass-2
     *     {@link org.opensearch.analytics.exec.QueryExecution} once {@code scheduler.execute}
     *     returns — used by {@code DefaultPlanExecutor.executeWithProfile} to populate its
     *     {@code execRef} so the profile listener can snapshot stage timings. May be null when
     *     profiling is disabled.
     * @param terminal fires on overall completion — success when the root stage emits joined
     *     rows, failure on any build-side or probe-side error.
     */
    public void run(
        QueryContext ctx,
        QueryDAG dag,
        Stage buildStage,
        Stage probeStage,
        Stage rootStage,
        Function<Stage, ExchangeSink> captureSinkFactory,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        assert buildStage.getRole() == Stage.StageRole.BROADCAST_BUILD : "BroadcastDispatch: buildStage role must be BROADCAST_BUILD";
        assert probeStage.getRole() == Stage.StageRole.BROADCAST_PROBE : "BroadcastDispatch: probeStage role must be BROADCAST_PROBE";

        // Capture sink is built for THIS build stage's output rowType (multi-broadcast resolves
        // each build with its own sink).
        ExchangeSink captureSink = captureSinkFactory.apply(buildStage);

        // Pass 1: run the build stage's ENTIRE sub-tree in isolation, feeding the build root's
        // output into captureSink. The build stage may itself be a COORDINATOR_REDUCE over shard
        // fragments (e.g. a decorrelated-EXISTS build side: Project(distinct) over a FINAL
        // aggregate over a PARTIAL aggregate). Building only the build node would leave its child
        // stages unscheduled, so the build reduce blocks forever in streamNext on input nobody
        // feeds. buildSubGraphWithSink recurses children + wires the cascade; we schedule the
        // leaves to kick off bottom-up dispatch, and listen on the build root (subGraph.root()).
        StageExecutionBuilder.SubGraph buildGraph = stageExecutionBuilder.buildSubGraphWithSink(
            buildStage,
            captureSink,
            ctx,
            scheduler::scheduleStage
        );
        StageExecution buildExec = buildGraph.root();

        // Captured in the state listener for the cancel-after-success race guard. Cancel-callback
        // installation happens lower down (and must — see the comment on setOnCancelCallback below).
        final AnalyticsQueryTask parentTask = ctx.parentTask();

        // Listener MUST be installed before setOnCancelCallback. setOnCancelCallback replays
        // synchronously when the task is already cancelled (see AnalyticsQueryTask:95-107) — if
        // a user cancel or cancel_after_time_interval landed during DAG rewrite, the replayed
        // callback transitions buildExec to CANCELLED. addStateListener does not replay terminal
        // states, so a listener registered after that point would never see the transition and
        // terminal.onFailure would never fire — leaving the broadcast query hung.
        buildExec.addStateListener((from, to) -> {
            switch (to) {
                case SUCCEEDED -> {
                    // P1 — close the capture sink. ShardFragmentStageExecution transitions to
                    // SUCCEEDED without calling outputSink.close() (the normal walker path has
                    // a parent stage that closes its own input sink; we don't have one here).
                    // Closing here completes ipcBytesFuture so extractIpcBytes() doesn't hang.
                    try {
                        captureSink.close();
                    } catch (Throwable closeErr) {
                        LOGGER.warn("[BroadcastDispatch] capture sink close threw", closeErr);
                        terminal.onFailure(new RuntimeException("BroadcastDispatch: capture sink close failed", closeErr));
                        return;
                    }

                    byte[] ipcBytes;
                    try {
                        ipcBytes = extractIpcBytes(captureSink);
                    } catch (Throwable t) {
                        LOGGER.warn("[BroadcastDispatch] failed to extract IPC bytes from capture sink", t);
                        terminal.onFailure(new RuntimeException("BroadcastDispatch: capture sink did not produce IPC bytes", t));
                        return;
                    }

                    // Guard against the cancel-after-success race: if a cancel landed between
                    // pass 1 SUCCEEDED and now, the pass-1 onCancel callback fired against the
                    // already-finished buildExec (a no-op). Without this check, pass 2 would
                    // still dispatch probe + root work on a query the user has already cancelled.
                    if (parentTask != null && parentTask.isCancelled()) {
                        String reason = parentTask.getReasonCancelled() != null ? parentTask.getReasonCancelled() : "unknown";
                        LOGGER.debug("[BroadcastDispatch] task cancelled after build SUCCEEDED, reason={}; aborting pass 2", reason);
                        terminal.onFailure(new TaskCancelledException("query cancelled: " + reason));
                        return;
                    }

                    try {
                        startPass2(ctx, dag, buildStage, probeStage, rootStage, ipcBytes, captureSinkFactory, queryExecutionSink, terminal);
                    } catch (Exception e) {
                        LOGGER.warn("[BroadcastDispatch] pass 2 failed to start", e);
                        terminal.onFailure(e);
                    } catch (Throwable t) {
                        LOGGER.warn("[BroadcastDispatch] pass 2 failed to start", t);
                        terminal.onFailure(new RuntimeException("BroadcastDispatch pass 2 failed to start", t));
                    }
                }
                case FAILED -> {
                    // P1 cleanup — close the sink on failure too so allocator buffers are released
                    // and any caller waiting on ipcBytesFuture (even defensively) completes.
                    try {
                        captureSink.close();
                    } catch (Throwable ignore) {
                        // Primary failure is being surfaced below.
                    }
                    Exception cause = buildExec.getFailure();
                    LOGGER.warn("[BroadcastDispatch] build stage failed", cause);
                    terminal.onFailure(cause != null ? cause : new RuntimeException("Build stage " + buildStage.getStageId() + " FAILED"));
                }
                case CANCELLED -> {
                    try {
                        captureSink.close();
                    } catch (Throwable ignore) {
                        // Primary cancel is being surfaced below.
                    }
                    LOGGER.debug("[BroadcastDispatch] build stage cancelled; skipping pass 2");
                    terminal.onFailure(new RuntimeException("Build stage " + buildStage.getStageId() + " CANCELLED"));
                }
                default -> {
                    // CREATED / RUNNING — no action.
                }
            }
        });

        // Wire cancellation for pass 1 AFTER the state listener is installed. The default query
        // lifecycle installs an onCancel callback in QueryScheduler.execute, but we bypass that
        // here, so user cancellation or cancel_after_time_interval would otherwise be silently
        // dropped while the build stage is running. Install a pass-1 callback that cancels the
        // build execution; pass 2's QueryScheduler.execute replaces this callback with its own
        // walker-level cancel when it starts.
        //
        // If the task is already cancelled at this point, setOnCancelCallback replays the
        // callback synchronously: buildExec.cancel(...) transitions to CANCELLED, the listener
        // (already installed above) fires, and terminal.onFailure resolves the query.
        if (parentTask != null) {
            parentTask.setOnCancelCallback(() -> {
                String reason = parentTask.getReasonCancelled() != null ? parentTask.getReasonCancelled() : "unknown";
                LOGGER.debug("[BroadcastDispatch] pass 1 cancel requested, reason={}", reason);
                try {
                    buildExec.cancel("task cancelled: " + reason);
                } catch (Exception e) {
                    LOGGER.warn("[BroadcastDispatch] failed to cancel build exec", e);
                }
            });
        }

        // If the task was cancelled and the cancel callback already drove buildExec to a
        // terminal state via the listener, don't kick off start() — transitionTo is a no-op
        // from a terminal state but emitting start() telemetry on a doomed exec is misleading.
        if (parentTask != null && parentTask.isCancelled()) {
            LOGGER.debug("[BroadcastDispatch] task already cancelled before pass 1 start; not invoking buildExec.start()");
            return;
        }

        LOGGER.debug(
            "[BroadcastDispatch] starting pass 1 (build stage {}, {} leaf stage(s))",
            buildStage.getStageId(),
            buildGraph.leaves().size()
        );
        // Kick off bottom-up dispatch by scheduling the build sub-tree's LEAVES. Each leaf's
        // SUCCEEDED cascades up to its parent (attachChildren wiring) until the build root runs
        // its reduce. Scheduling the root directly (the pre-fix behaviour) only works when the
        // build stage is itself a leaf; for a multi-stage build sub-tree the children must run
        // first. scheduleStage materializes + dispatches each leaf's tasks (start() alone would
        // only transition to RUNNING without invoking TaskRunner.run).
        for (StageExecution leaf : buildGraph.leaves()) {
            scheduler.scheduleStage(leaf);
        }
    }

    /**
     * Tripwire timeout for the capture-sink contract. The dispatcher invokes
     * {@link #extractIpcBytes} only after {@code captureSink.close()} has returned, and the
     * SPI contract for capture sinks requires {@code close()} to settle {@code ipcBytesFuture}
     * synchronously before returning. Healthy completion is therefore microseconds; this bound
     * exists solely to surface a sink-contract violation (an async or buggy {@code close()}
     * that returns without completing the future) instead of blocking the dispatcher
     * indefinitely. 30s rides out worst-case GC pauses and scheduling jitter while still
     * failing fast enough to be diagnosable.
     */
    private static final long EXTRACT_IPC_TIMEOUT_SECONDS = 30L;

    /**
     * Extracts IPC bytes from the capture sink. Uses reflection to keep analytics-engine free
     * of a compile-time dependency on the DataFusion backend (the only current implementer of
     * {@code ipcBytesFuture()}). Expects the sink to implement a method
     * {@code CompletableFuture<byte[]> ipcBytesFuture()}, completed synchronously by its
     * {@code close()} (which the dispatcher has already invoked before calling this method).
     */
    @SuppressWarnings("unchecked")
    private byte[] extractIpcBytes(ExchangeSink captureSink) throws Exception {
        Method m = captureSink.getClass().getMethod("ipcBytesFuture");
        CompletableFuture<byte[]> fut = (CompletableFuture<byte[]>) m.invoke(captureSink);
        try {
            return fut.get(EXTRACT_IPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException te) {
            // Don't block the dispatcher forever on a sink that violates the contract.
            // FutureUtils.cancel routes through cancel(false) — no producer thread to interrupt
            // here, this just marks the future done so any later listener doesn't leak. (Direct
            // Future.cancel(boolean) is forbidden by OpenSearch's forbidden-apis policy.)
            FutureUtils.cancel(fut);
            throw new RuntimeException(
                "BroadcastDispatch: capture sink did not complete ipcBytesFuture within "
                    + EXTRACT_IPC_TIMEOUT_SECONDS
                    + "s — likely a sink-contract violation "
                    + "(close() must complete the future synchronously); sink="
                    + captureSink.getClass().getName(),
                te
            );
        }
    }

    /**
     * Pass 2: mutate the probe stage's plan alternatives to append the broadcast instruction,
     * then build a derived DAG whose probe is childless (build already succeeded) and hand it
     * to the normal {@link QueryScheduler#execute} path.
     */
    private void startPass2(
        QueryContext ctx,
        QueryDAG dag,
        Stage buildStage,
        Stage probeStage,
        Stage rootStage,
        byte[] ipcBytes,
        Function<Stage, ExchangeSink> captureSinkFactory,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        int buildSideIndex = deriveBuildSideIndex(rootStage, buildStage);
        enrichProbeAlternatives(probeStage, buildStage.getStageId(), buildSideIndex, ipcBytes);
        LOGGER.debug(
            "[BroadcastDispatch] prepared probe plan alternatives with broadcast instruction ({} bytes, buildSideIndex={})",
            ipcBytes.length,
            buildSideIndex
        );

        // Build pass-2 DAG: walk from the original root, replacing only the probe stage
        // with a leaf copy (its build child has already run to completion). Sibling stages
        // are preserved so multi-input root shapes — e.g. UNION with a broadcast-join in
        // one branch and a non-broadcast scan in another — keep the non-broadcast stages
        // intact. An earlier version rebuilt the root with a single child (just the probe),
        // silently dropping siblings while the root fragment still referenced their stage
        // inputs, producing missing rows or execution failure.
        Stage pass2Root = rebuildPass2(dag.rootStage(), probeStage);
        QueryDAG pass2Dag = new QueryDAG(dag.queryId(), pass2Root);
        QueryContext pass2Ctx = ctx.withDag(pass2Dag);

        // Multi-broadcast: a query with several independent broadcast joins (e.g. TPC-H q2 with
        // part⋈partsupp⋈supplier⋈nation⋈region) produces a DAG with N BROADCAST_BUILD stages. We
        // resolve them one at a time — this iteration just stripped one build/probe pair; if the
        // rebuilt DAG still contains a BROADCAST_BUILD, recurse to resolve the next before the
        // final scheduler.execute. Each level captures its own build's IPC into a fresh sink.
        Stage nextBuild = JoinStrategyAdvisor.findBroadcastBuild(pass2Dag);
        if (nextBuild != null) {
            Stage nextProbe = findProbeForBuild(pass2Root, nextBuild.getStageId());
            if (nextProbe == null) {
                throw new IllegalStateException(
                    "BroadcastDispatch: no BROADCAST_PROBE found for residual build stage " + nextBuild.getStageId()
                );
            }
            LOGGER.debug(
                "[BroadcastDispatch] residual broadcast build {} (probe {}) — recursing for next pass",
                nextBuild.getStageId(),
                nextProbe.getStageId()
            );
            run(pass2Ctx, pass2Dag, nextBuild, nextProbe, pass2Root, captureSinkFactory, queryExecutionSink, terminal);
            return;
        }

        LOGGER.debug("[BroadcastDispatch] starting pass 2 (probe {} + root {})", probeStage.getStageId(), rootStage.getStageId());
        QueryExecution exec = scheduler.execute(pass2Ctx, terminal);
        if (queryExecutionSink != null) {
            queryExecutionSink.accept(exec);
        }
    }

    /**
     * Finds the BROADCAST_PROBE stage paired with {@code buildStageId} — the stage whose fragment
     * contains an {@link org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan} referencing
     * that build. Pairing is by build-stage id (not role position), so independent broadcast joins
     * in the same DAG resolve to the correct probe.
     */
    private static Stage findProbeForBuild(Stage stage, int buildStageId) {
        if (stage.getRole() == Stage.StageRole.BROADCAST_PROBE
            && RelNodeUtils.findNodes(stage.getFragment(), OpenSearchBroadcastScan.class)
                .stream()
                .anyMatch(scan -> scan.getBuildStageId() == buildStageId)) {
            return stage;
        }
        for (Stage child : stage.getChildStages()) {
            Stage found = findProbeForBuild(child, buildStageId);
            if (found != null) return found;
        }
        return null;
    }

    /**
     * Appends a {@link BroadcastInjectionInstructionNode} to every plan alternative on the probe
     * stage. Exposed package-private for unit testing — the full dispatch path is end-to-end so
     * this is the most surgical hook for verifying the mutation logic in isolation.
     */
    static void enrichProbeAlternatives(Stage probeStage, int buildStageId, int buildSideIndex, byte[] ipcBytes) {
        String namedInputId = "broadcast-" + buildStageId;
        List<StagePlan> enriched = new ArrayList<>(probeStage.getPlanAlternatives().size());
        for (StagePlan sp : probeStage.getPlanAlternatives()) {
            List<InstructionNode> existing = sp.instructions();
            List<InstructionNode> merged = new ArrayList<>(existing.size() + 1);
            merged.addAll(existing);
            merged.add(new BroadcastInjectionInstructionNode(namedInputId, buildSideIndex, ipcBytes));
            enriched.add(sp.withInstructions(merged));
        }
        probeStage.setPlanAlternatives(enriched);
    }

    /**
     * buildSideIndex = 0 when the join's left subtree was the build side, else 1. The probe
     * stage's {@code OpenSearchJoin} preserves the original left/right ordering from CBO; the
     * memtable is looked up by name on the data-node side, not by index, so the value is
     * primarily informational for logging/telemetry. A future phase that gates build-side row
     * preservation by index will need to thread this through properly.
     */
    private int deriveBuildSideIndex(Stage rootStage, Stage buildStage) {
        return 0;
    }

    /**
     * Rebuilds the pass-2 DAG by walking from {@code source} and replacing the probe stage
     * with a leaf copy (its already-run build child stripped). Every other stage is copied
     * with its children rebuilt recursively, so siblings of the probe are preserved.
     *
     * <p>This is identity-comparing the probe stage instance (the original probe handed to
     * {@link #run}, before this rebuild). Pass-2 dispatch sees that the probe still has its
     * shard target resolver + instruction-enriched plan alternatives but no children — the
     * walker treats it as a shard-fragment leaf.
     */
    private Stage rebuildPass2(Stage source, Stage probeStage) {
        if (source == probeStage) {
            return copyAsLeaf(source);
        }
        List<Stage> originalChildren = source.getChildStages();
        if (originalChildren.isEmpty()) {
            // Already a leaf — copy unchanged. Cheaper than a full rebuild call.
            return copyAsLeaf(source);
        }
        List<Stage> rebuiltChildren = new ArrayList<>(originalChildren.size());
        for (Stage child : originalChildren) {
            rebuiltChildren.add(rebuildPass2(child, probeStage));
        }
        Stage copy = new Stage(
            source.getStageId(),
            source.getFragment(),
            rebuiltChildren,
            source.getExchangeInfo(),
            source.getExchangeSinkProvider(),
            source.getTargetResolver()
        );
        copy.setRole(source.getRole());
        copy.setPlanAlternatives(source.getPlanAlternatives());
        if (source.getInstructionHandlerFactory() != null) {
            copy.setInstructionHandlerFactory(source.getInstructionHandlerFactory());
        }
        return copy;
    }

    private Stage copyAsLeaf(Stage source) {
        Stage copy = new Stage(
            source.getStageId(),
            source.getFragment(),
            List.of(),
            source.getExchangeInfo(),
            source.getExchangeSinkProvider(),
            source.getTargetResolver()
        );
        copy.setRole(source.getRole());
        copy.setPlanAlternatives(source.getPlanAlternatives());
        if (source.getInstructionHandlerFactory() != null) {
            copy.setInstructionHandlerFactory(source.getInstructionHandlerFactory());
        }
        return copy;
    }
}
