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
            // Per-shard hasDeletions signal, stamped by AnalyticsSearchService from the Lucene backend's
            // hasDeletedDocs probe. When true and the query has no delegation, route the pure-DF scan
            // through the indexed SingleCollector path (CONJUNCTIVE): the native executor ANDs a
            // synthetic match-all Collector (reserved annotation id, resolved by the Lucene handle to
            // the segment's liveDocs) into the decoded filter tree, so deleted rows are excluded via
            // the ordinary collector machinery. When false, the vanilla ListingTable path runs with
            // zero extra work.
            boolean deletedDocFilteringRequired = context.hasDeletedDocs();
            SessionContextHandle sessionCtxHandle;
            if (node.requestsRowIds()) {
                // QTF query phase — narrowed scan emits __row_id__. Use the indexed session
                // context so the IndexedTableProvider injects shard-global row ids during scan.
                // No delegated predicates here (delegation goes through ShardScanWithDelegationHandler),
                // so delegatedPredicateCount=0. When the shard has deletions, route through
                // SingleCollector (CONJUNCTIVE) so the injected match-all Collector excludes deleted
                // docs from candidates before the row-ids are emitted. Otherwise NO_DELEGATION →
                // PredicateOnlyEvaluator (no liveDocs work).
                int rowIdTreeShape = deletedDocFilteringRequired
                    ? FilterTreeShape.CONJUNCTIVE.ordinal()
                    : FilterTreeShape.NO_DELEGATION.ordinal();
                sessionCtxHandle = NativeBridge.createSessionContextForIndexedExecution(
                    readerPtr,
                    runtimePtr,
                    tableName,
                    contextId,
                    rowIdTreeShape,
                    0,
                    true,
                    deletedDocFilteringRequired,
                    context.hasPartialAggregate(),
                    segment.address(),
                    context.getFragmentBytes()
                );
            } else if (deletedDocFilteringRequired) {
                // Pure-DF query on a shard with deletions: force the indexed SingleCollector path
                // (CONJUNCTIVE, 0 delegated, no row-ids). The native executor injects the match-all
                // Collector into the decoded tree, so per-RG candidates are exactly the live docs
                // (optionally intersected with the query's own predicates as residual).
                sessionCtxHandle = NativeBridge.createSessionContextForIndexedExecution(
                    readerPtr,
                    runtimePtr,
                    tableName,
                    contextId,
                    FilterTreeShape.CONJUNCTIVE.ordinal(),
                    0,
                    false,
                    true,
                    context.hasPartialAggregate(),
                    segment.address(),
                    context.getFragmentBytes()
                );
            } else {
                // Plan bytes let Rust widen the schema for multi-index queries (null-fill missing columns).
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
