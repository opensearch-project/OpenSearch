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
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StagePlan;
import org.opensearch.analytics.spi.BroadcastInjectionInstructionNode;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Two-pass orchestrator for M1 broadcast-join execution.
 *
 * <p>Drives the broadcast-shape {@link QueryDAG} produced by {@link BroadcastDAGRewriter}
 * through two sequential phases against the shared {@link StageExecutionBuilder} and
 * {@link QueryScheduler}:
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
     * Drives the rewritten DAG through pass 1 + pass 2.
     *
     * @param ctx the query context whose DAG is {@code rewrittenDag} (pass 2 will use a derived
     *     context whose DAG drops the build stage).
     * @param rewrittenDag the broadcast-shape DAG from {@link BroadcastDAGRewriter#rewrite}.
     * @param buildStage the build child of the rewritten probe stage. Role must be
     *     {@link Stage.StageRole#BROADCAST_BUILD}.
     * @param probeStage the probe stage in the rewritten DAG. Role must be
     *     {@link Stage.StageRole#BROADCAST_PROBE}.
     * @param rootStage the root stage in the rewritten DAG.
     * @param captureSinkFactory creates the backend-specific IPC capture sink. Typically
     *     {@code () -> new BroadcastCaptureSink(ctx.bufferAllocator())}. The factory returns an
     *     {@link ExchangeSink} that additionally exposes a {@code CompletableFuture&lt;byte[]&gt;}
     *     via {@code ipcBytesFuture()}; the dispatcher retrieves IPC bytes via reflection on that
     *     method to keep analytics-engine from depending on the DataFusion backend directly.
     * @param terminal fires on overall completion — success when the root stage emits joined
     *     rows, failure on any build-side or probe-side error.
     */
    public void run(
        QueryContext ctx,
        QueryDAG rewrittenDag,
        Stage buildStage,
        Stage probeStage,
        Stage rootStage,
        Supplier<ExchangeSink> captureSinkFactory,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        assert buildStage.getRole() == Stage.StageRole.BROADCAST_BUILD : "BroadcastDispatch: buildStage role must be BROADCAST_BUILD";
        assert probeStage.getRole() == Stage.StageRole.BROADCAST_PROBE : "BroadcastDispatch: probeStage role must be BROADCAST_PROBE";

        ExchangeSink captureSink = captureSinkFactory.get();

        // Pass 1: isolate the build stage, feed its output into captureSink.
        StageExecution buildExec = stageExecutionBuilder.buildWithSink(buildStage, captureSink, ctx);

        // Captured in the state listener for the cancel-after-success race guard. Cancel-callback
        // installation happens lower down (and must — see the comment on setOnCancelCallback below).
        final org.opensearch.analytics.exec.task.AnalyticsQueryTask parentTask = ctx.parentTask();

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
                        terminal.onFailure(new org.opensearch.core.tasks.TaskCancelledException("query cancelled: " + reason));
                        return;
                    }

                    try {
                        startPass2(ctx, rewrittenDag, buildStage, probeStage, rootStage, ipcBytes, terminal);
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

        LOGGER.debug("[BroadcastDispatch] starting pass 1 (build stage {})", buildStage.getStageId());
        buildExec.start();
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
        java.lang.reflect.Method m = captureSink.getClass().getMethod("ipcBytesFuture");
        java.util.concurrent.CompletableFuture<byte[]> fut = (java.util.concurrent.CompletableFuture<byte[]>) m.invoke(captureSink);
        try {
            return fut.get(EXTRACT_IPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException te) {
            // Don't block the dispatcher forever on a sink that violates the contract.
            // cancel(false) doesn't interrupt anything (no producer thread to interrupt) —
            // it just marks the future done so any later listener doesn't leak.
            fut.cancel(false);
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
        QueryDAG rewrittenDag,
        Stage buildStage,
        Stage probeStage,
        Stage rootStage,
        byte[] ipcBytes,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        int buildSideIndex = deriveBuildSideIndex(rootStage, buildStage);
        enrichProbeAlternatives(probeStage, buildStage.getStageId(), buildSideIndex, ipcBytes);
        LOGGER.debug(
            "[BroadcastDispatch] prepared probe plan alternatives with broadcast instruction ({} bytes, buildSideIndex={})",
            ipcBytes.length,
            buildSideIndex
        );

        // Build pass-2 DAG: root → probe (childless). Root's fragment / sink / instruction
        // handler factory / plan alternatives all carry over untouched; we're just stripping
        // the probe stage's reference to the build child (which has already run to completion).
        Stage pass2Probe = copyAsLeaf(probeStage);
        Stage pass2Root = copyWithSingleChild(rootStage, pass2Probe);
        QueryDAG pass2Dag = new QueryDAG(rewrittenDag.queryId(), pass2Root);
        QueryContext pass2Ctx = ctx.withDag(pass2Dag);

        LOGGER.debug("[BroadcastDispatch] starting pass 2 (probe {} + root {})", probeStage.getStageId(), rootStage.getStageId());
        scheduler.execute(pass2Ctx, terminal);
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
     * buildSideIndex = 0 when the root join's left subtree was the build side, else 1. For the
     * rewritten DAG the root's single child is the probe stage, but the original build/probe
     * ordering is preserved in the probe stage's {@code OpenSearchJoin}. The advisor already
     * tagged the child stages before rewriting, so we can recover the original side from the
     * {@code buildStage} — but for the instruction's purposes, the coord-tag 'left/right' is
     * relative to the root join's inputs, which the rewriter recomputed. We default to 0 here:
     * the probe-side handler looks up the memtable by name, not by index, so the value is
     * primarily informational for logging/telemetry in M1. A future phase that actually gates
     * build-side row preservation by index will need to thread this through properly.
     */
    private int deriveBuildSideIndex(Stage rootStage, Stage buildStage) {
        // TODO (M1.5): thread through the original join's left/right orientation from the
        // advisor so outer-join row-preservation pinning is exposed to the handler. For M1
        // broadcast-to-probe the memtable is looked up by name and the join condition is
        // preserved from the original OpenSearchJoin, so the index value is not load-bearing.
        return 0;
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

    private Stage copyWithSingleChild(Stage source, Stage newChild) {
        Stage copy = new Stage(
            source.getStageId(),
            source.getFragment(),
            List.of(newChild),
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
