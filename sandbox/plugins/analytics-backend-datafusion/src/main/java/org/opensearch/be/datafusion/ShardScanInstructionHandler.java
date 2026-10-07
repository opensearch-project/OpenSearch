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
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Handles ShardScan instruction: creates a SessionContext via FFM and registers
 * the default ListingTable provider for parquet scans.
 */
public class ShardScanInstructionHandler implements FragmentInstructionHandler<ShardScanInstructionNode> {

    private final DataFusionPlugin plugin;

    ShardScanInstructionHandler(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public BackendExecutionContext apply(
        ShardScanInstructionNode node,
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
        // The coordinator captured the logical table name (alias / index pattern / index the query
        // referenced) from the plan's table-scan leaf. Register the shard's table under it so the
        // Substrait plan's NamedTable binds. Fall back to the concrete shard index name when absent.
        String tableName = node.getLogicalTableName() != null ? node.getLogicalTableName() : context.getTableName();

        WireConfigSnapshot snapshot = plugin.getDatafusionSettings().getSnapshot();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);
            boolean requestsRowIds = node.requestsRowIds();
            // Per-shard hasDeletions signal (stamped by AnalyticsSearchService). Deletions force the
            // indexed SingleCollector path (CONJUNCTIVE) so the synthetic live-docs collector filters
            // deleted docs from candidates; row-ids then index into the live-only bitmap.
            boolean requiresLiveDocs = context.hasDeletedDocs();
            SessionContextHandle sessionCtxHandle;
            // Indexed execution is required either for QTF row IDs or for liveDocs masking. No
            // delegated predicates here (delegation goes through ShardScanWithDelegationHandler), so
            // delegatedPredicateCount=0. Otherwise the vanilla ListingTable path runs with zero extra
            // work (its plan bytes let Rust widen the schema for multi-index queries).
            if (requestsRowIds || requiresLiveDocs) {
                int treeShape = requiresLiveDocs
                    ? FilterTreeShape.CONJUNCTIVE.ordinal()
                    : FilterTreeShape.NO_DELEGATION.ordinal();
                sessionCtxHandle = NativeBridge.createSessionContextForIndexedExecution(
                    readerPtr,
                    runtimePtr,
                    tableName,
                    contextId,
                    treeShape,
                    0,
                    requestsRowIds,
                    requiresLiveDocs,
                    context.hasPartialAggregate(),
                    segment.address(),
                    context.getFragmentBytes()
                );
            } else {
                sessionCtxHandle = NativeBridge.createSessionContext(
                    readerPtr,
                    runtimePtr,
                    tableName,
                    contextId,
                    false,
                    context.hasPartialAggregate(),
                    segment.address(),
                    context.getFragmentBytes()
                );
            }
            return new DataFusionSessionState(sessionCtxHandle);
        }
    }
}
