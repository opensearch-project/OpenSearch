/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShuffleBufferAccess;
import org.opensearch.analytics.spi.ShuffleBufferRegistry;
import org.opensearch.analytics.spi.ShuffleWorkerSetupInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Handles {@link ShuffleWorkerSetupInstructionNode}: bootstraps a worker-mode DataFusion
 * SessionContext (no shard view, no listing table) so subsequent {@link
 * org.opensearch.be.datafusion.ShuffleScanHandler}s can register named-input streams against
 * it. Returns a {@link DataFusionSessionState} carrying the new {@link SessionContextHandle}.
 */
public class ShuffleWorkerSetupHandler implements FragmentInstructionHandler<ShuffleWorkerSetupInstructionNode> {

    private final DataFusionPlugin plugin;

    ShuffleWorkerSetupHandler(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public BackendExecutionContext apply(
        ShuffleWorkerSetupInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        ShardScanExecutionContext context = (ShardScanExecutionContext) commonContext;
        // Set both sides' expected sender counts eagerly. The buffer must know both counts
        // BEFORE any subsequent ShuffleScanHandler calls awaitReady — otherwise the second
        // side's latch never fires (CountDownLatch with -1 expected stays blocked).
        ShuffleBufferRegistry registry = context.getShuffleBufferRegistry();
        if (registry != null) {
            ShuffleBufferAccess buffer = registry.getOrCreate(node.getQueryId(), node.getTargetStageId(), node.getPartitionIndex());
            buffer.setExpectedSenders(node.getLeftExpectedSenders(), node.getRightExpectedSenders());
        }
        DataFusionService dataFusionService = plugin.getDataFusionService();
        long runtimePtr = dataFusionService.getNativeRuntime().get();
        long contextId = context.getTask() != null ? context.getTask().getId() : 0L;
        // The node-global snapshot carries the default (prefer_hash_join=true); the coordinator's
        // per-worker-stage decision on this instruction overrides it, so a large-build worker join
        // gets a spillable sort-merge join while other stages keep the hash-join default.
        WireConfigSnapshot snapshot = WireConfigSnapshot.builder(plugin.getDatafusionSettings().getSnapshot())
            .preferHashJoin(node.getPreferHashJoin())
            .build();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);
            SessionContextHandle handle = NativeBridge.createWorkerSessionContext(runtimePtr, contextId, segment.address());
            return new DataFusionSessionState(handle);
        }
    }
}
