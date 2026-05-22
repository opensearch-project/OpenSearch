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
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShardScanWithDelegationInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Handles ShardScanWithDelegation instruction: creates a SessionContext via FFM
 * configured for indexed execution — registers the delegated_predicate UDF and
 * sets up the custom scan operator (IndexedTableProvider) with FilterTreeShape
 * and delegatedPredicateCount.
 */
public class ShardScanWithDelegationHandler implements FragmentInstructionHandler<ShardScanWithDelegationInstructionNode> {

    private final DataFusionPlugin plugin;

    ShardScanWithDelegationHandler(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public BackendExecutionContext apply(
        ShardScanWithDelegationInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        ShardScanExecutionContext context = (ShardScanExecutionContext) commonContext;
        DataFusionService dataFusionService = plugin.getDataFusionService();
        DataFormatRegistry registry = plugin.getDataFormatRegistry();

        DatafusionReader dfReader = null;
        for (String formatName : plugin.getSupportedFormats()) {
            dfReader = context.getReader().getReader(registry.format(formatName), DatafusionReader.class);
            if (dfReader != null) break;
        }
        if (dfReader == null) {
            throw new IllegalStateException("No DatafusionReader available in the acquired reader");
        }

        long readerPtr = dfReader.getReaderHandle().getPointer();
        long runtimePtr = dataFusionService.getNativeRuntime().get();
        long contextId = context.getTask() != null ? context.getTask().getId() : 0L;
        FilterTreeShape treeShape = node.getTreeShape();
        int delegatedPredicateCount = node.getDelegatedPredicateCount();

        WireConfigSnapshot snapshot = plugin.getDatafusionSettings().getSnapshot();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);
            SessionContextHandle sessionCtxHandle = NativeBridge.createSessionContextForIndexedExecution(
                readerPtr,
                runtimePtr,
                context.getTableName(),
                contextId,
                treeShape.ordinal(),
                delegatedPredicateCount,
                segment.address()
            );
            return new DataFusionSessionState(sessionCtxHandle);
        }
    }
}
