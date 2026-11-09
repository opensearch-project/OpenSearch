/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.CanMatchFilterSerializer;
import org.opensearch.analytics.exec.canmatch.LongRange;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin.CanMatchResult;
import org.opensearch.analytics.spi.ShardSortBounds;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.shard.IndexShard;

import java.util.List;

/**
 * Evaluates can-match range predicates against parquet row-group statistics.
 * Acquires a reader from the shard to get the native shard_view_ptr, then
 * delegates to the Rust-side df_can_match which iterates all parquet files
 * internally.
 *
 * <p>AND semantics across filters: every filter must pass. One FFM call per
 * filter — Rust iterates files internally with OR semantics (any file
 * overlapping means the filter passes).
 *
 * <p>Fail-open on any error — returns true (keep shard).
 *
 * @opensearch.internal
 */
final class ParquetRangeEvaluator {

    private static final Logger logger = LogManager.getLogger(ParquetRangeEvaluator.class);

    private ParquetRangeEvaluator() {}

    /**
     * Evaluates whether the shard can match the given filters.
     *
     * @return true if the shard might match (or cannot determine), false if provably cannot
     */
    static boolean evaluate(IndexShard shard, byte[] filterBytes, DataFusionPlugin plugin) {
        return evaluateWithBounds(shard, filterBytes, null, plugin).canMatch();
    }

    /**
     * Evaluates the prune predicate and, for a surviving shard, folds {@code sortColumn}'s
     * min/max — both under a single reader acquisition, since both read the same parquet footers.
     *
     * <p>Prune first and short-circuit: a pruned shard is never dispatched, so nothing reads its
     * bounds, and the fold is the costlier call (every file and row group, versus a prune check
     * that can exit on the first overlap).
     *
     * <p>Fails open on every path, logging the cause at DEBUG. Null bounds are not necessarily a
     * failure — the fold legitimately returns nothing for a column with no usable statistics.
     */
    static CanMatchResult evaluateWithBounds(IndexShard shard, byte[] filterBytes, String sortColumn, DataFusionPlugin plugin) {
        try {
            List<CanMatchFilter> filters = CanMatchFilterSerializer.deserialize(filterBytes);
            // Nothing to prune, nothing to measure — don't even acquire a reader.
            if (filters.isEmpty() && sortColumn == null) {
                return CanMatchResult.matched(null);
            }

            DataFusionService svc = plugin.getDataFusionService();
            if (svc == null) {
                logger.debug("can-match: DataFusion service unavailable (fail-open)");
                return CanMatchResult.unavailable();
            }
            NativeRuntimeHandle runtimeHandle = svc.getNativeRuntime();
            if (runtimeHandle == null) {
                logger.debug("can-match: native runtime unavailable (fail-open)");
                return CanMatchResult.unavailable();
            }
            long runtimePtr = runtimeHandle.get();
            IndexReaderProvider readerProvider = shard.getReaderProvider();
            if (readerProvider == null) {
                logger.debug("can-match: shard {} has no reader provider (fail-open)", shard.shardId());
                return CanMatchResult.unavailable();
            }

            try (GatedCloseable<Reader> gatedReader = readerProvider.acquireReader()) {
                Reader reader = gatedReader.get();
                if (reader == null) {
                    logger.debug("can-match: shard {} reader unavailable (fail-open)", shard.shardId());
                    return CanMatchResult.unavailable();
                }

                long shardViewPtr = resolveShardViewPtr(reader, plugin);
                if (shardViewPtr == 0) {
                    logger.debug("can-match: shard {} has no native shard view (fail-open)", shard.shardId());
                    return CanMatchResult.unavailable();
                }

                // AND across filters: all must pass
                for (CanMatchFilter filter : filters) {
                    if (filter instanceof LongRange range) {
                        long result = NativeBridge.canMatch(runtimePtr, shardViewPtr, range.column(), range.min(), range.max());
                        // Only a definite NO prunes; YES and UNKNOWN both keep the shard.
                        if (result == NativeBridge.CAN_MATCH_NO) {
                            // Pruned, so nobody will read this shard's bounds — skip the fold.
                            return CanMatchResult.pruned();
                        }
                    }
                }

                if (sortColumn == null) {
                    return CanMatchResult.matched(null);
                }
                // Keep the prune answer we already have even if the fold blows up.
                ShardSortBounds bounds;
                try {
                    bounds = NativeBridge.shardSortBounds(runtimePtr, shardViewPtr, sortColumn);
                } catch (Exception e) {
                    logger.error("can-match: sort-bounds fold failed for column {} (fail-open):", sortColumn, e);
                    return CanMatchResult.unavailable();
                }
                // null means the column has no usable range — a real answer, not a failure.
                return CanMatchResult.matched(bounds);
            }
        } catch (Exception e) {
            logger.error("can-match evaluation failed, returning true (fail-open):", e);
            return CanMatchResult.unavailable();
        }
    }

    private static long resolveShardViewPtr(Reader reader, DataFusionPlugin plugin) {
        try {
            DatafusionReader dfReader = null;
            DataFormatRegistry registry = plugin.getDataFormatRegistry();
            for (String formatName : plugin.getSupportedFormats()) {
                dfReader = reader.getReader(registry.format(formatName), DatafusionReader.class);
                if (dfReader != null) break;
            }
            if (dfReader == null) {
                return 0;
            }
            return dfReader.getReaderHandle().getPointer();
        } catch (Exception e) {
            return 0;
        }
    }
}
