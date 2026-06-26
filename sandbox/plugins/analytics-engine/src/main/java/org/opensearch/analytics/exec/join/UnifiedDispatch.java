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
import java.util.function.Consumer;
import java.util.function.Function;

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

    public UnifiedDispatch(
        QueryScheduler scheduler,
        ClusterService clusterService,
        CapabilityRegistry capabilityRegistry,
        boolean preferMetadataDriver
    ) {
        this.scheduler = scheduler;
        this.clusterService = clusterService;
        this.capabilityRegistry = capabilityRegistry;
        this.preferMetadataDriver = preferMetadataDriver;
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
            List<Stage> builds = new ArrayList<>();
            collectBuildStages(dag.rootStage(), builds);
            if (builds.isEmpty()) {
                dispatchBroadcastFree(ctx, dag, Function.identity(), queryExecutionSink, terminal);
                return;
            }
            captureThenDispatch(ctx, dag, builds, captureSinkFactory, queryExecutionSink, terminal);
        } catch (Exception e) {
            terminal.onFailure(e);
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
            ShuffleEnrichment.enrichLevels(rewritten.levels(), ctx, clusterService, capabilityRegistry);
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
     * Capture phase: build every broadcast subtree in isolation, collect IPC keyed by
     * {@code broadcast-<buildStageId>}, then inject + strip + dispatch the broadcast-free residual. One
     * shared cancel callback is installed before any scheduling; dispatch kicks off bottom-up via the
     * build leaves; a once-only terminal guards the multiple build listeners + the residual dispatch.
     */
    private void captureThenDispatch(
        QueryContext ctx,
        QueryDAG dag,
        List<Stage> builds,
        Function<Stage, ExchangeSink> captureSinkFactory,
        Consumer<QueryExecution> queryExecutionSink,
        ActionListener<Iterable<VectorSchemaRoot>> terminal
    ) {
        StageExecutionBuilder builder = scheduler.getStageExecutionBuilder();
        Map<Integer, byte[]> capturedByBuildId = new LinkedHashMap<>();
        AtomicInteger remaining = new AtomicInteger(builds.size());
        List<StageExecution> buildRoots = new ArrayList<>(builds.size());
        List<StageExecution> allLeaves = new ArrayList<>();

        for (Stage buildStage : builds) {
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
                        if (remaining.decrementAndGet() == 0) {
                            if (ctx.parentTask() != null && ctx.parentTask().isCancelled()) {
                                String reason = ctx.parentTask().getReasonCancelled() != null
                                    ? ctx.parentTask().getReasonCancelled()
                                    : "unknown";
                                terminal.onFailure(new TaskCancelledException("query cancelled during broadcast builds: " + reason));
                                return;
                            }
                            try {
                                injectStripAndDispatch(ctx, dag, capturedByBuildId, queryExecutionSink, terminal);
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

        // Cancel wiring for the capture phase AFTER every listener is installed (it bypasses the normal
        // QueryScheduler.execute cancel path by scheduling build leaves directly). Phase-2's execute
        // replaces this callback with its own walker-level cancel.
        AnalyticsQueryTask parentTask = ctx.parentTask();
        if (parentTask != null) {
            parentTask.setOnCancelCallback(() -> {
                String reason = parentTask.getReasonCancelled() != null ? parentTask.getReasonCancelled() : "unknown";
                LOGGER.debug("[UnifiedDispatch] capture phase cancel requested, reason={}", reason);
                for (StageExecution buildExec : buildRoots) {
                    try {
                        buildExec.cancel("task cancelled: " + reason);
                    } catch (Exception e) {
                        LOGGER.warn("[UnifiedDispatch] failed to cancel build exec", e);
                    }
                }
            });
        }
        if (parentTask != null && parentTask.isCancelled()) {
            LOGGER.debug("[UnifiedDispatch] task already cancelled before capture start; not scheduling builds");
            return;
        }

        for (StageExecution leaf : allLeaves) {
            scheduler.scheduleStage(leaf);
        }
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
        ActionListener<Iterable<VectorSchemaRoot>> terminal
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
        Stage copy = new Stage(
            stage.getStageId(),
            stage.getFragment(),
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

    /** Collects every {@link Stage.StageRole#BROADCAST_BUILD} stage in the DAG (multi-broadcast support). */
    private static void collectBuildStages(Stage stage, List<Stage> out) {
        if (stage.getRole() == Stage.StageRole.BROADCAST_BUILD) {
            out.add(stage);
        }
        for (Stage child : stage.getChildStages()) {
            collectBuildStages(child, out);
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
        Stage root = dag.rootStage();
        List<String> reduceViable = CapabilityResolutionUtils.filterByReduceCapability(
            capabilityRegistry,
            ((OpenSearchRelNode) root.getFragment()).getViableBackends()
        );
        if (reduceViable.isEmpty()) {
            throw new IllegalStateException("No reduce-capable backend for broadcast capture sink (general scheduler)");
        }
        final String captureBackendId = reduceViable.get(0);
        final long broadcastMaxBytes = clusterService.getClusterSettings().get(AnalyticsSettings.BROADCAST_MAX_BYTES).getBytes();
        return stage -> capabilityRegistry.getBackend(captureBackendId)
            .getExchangeSinkProvider()
            .createBroadcastCaptureSink(ctx.bufferAllocator(), stage.getFragment().getRowType(), broadcastMaxBytes);
    }
}
