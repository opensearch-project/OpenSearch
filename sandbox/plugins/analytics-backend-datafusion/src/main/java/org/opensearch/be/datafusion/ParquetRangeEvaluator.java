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
        try {
            List<CanMatchFilter> filters = CanMatchFilterSerializer.deserialize(filterBytes);
            if (filters.isEmpty()) {
                return true;
            }

            DataFusionService svc = plugin.getDataFusionService();
            if (svc == null) {
                return true;
            }
            NativeRuntimeHandle runtimeHandle = svc.getNativeRuntime();
            if (runtimeHandle == null) {
                return true;
            }
            long runtimePtr = runtimeHandle.get();
            IndexReaderProvider readerProvider = shard.getReaderProvider();
            if (readerProvider == null) {
                return true;
            }

            try (GatedCloseable<Reader> gatedReader = readerProvider.acquireReader()) {
                Reader reader = gatedReader.get();
                if (reader == null) {
                    return true;
                }

                long shardViewPtr = resolveShardViewPtr(reader, plugin);
                if (shardViewPtr == 0) {
                    return true;
                }

                // AND across filters: all must pass
                for (CanMatchFilter filter : filters) {
                    if (filter instanceof LongRange range) {
                        long result = NativeBridge.canMatch(
                            runtimePtr, shardViewPtr,
                            range.column(), range.min(), range.max()
                        );
                        if (result == 0L) {
                            return false; // all files disjoint for this filter → prune
                        }
                    }
                }
                return true;
            }
        } catch (Exception e) {
            logger.debug("can-match evaluation failed, returning true (fail-open): {}", e.getMessage());
            return true;
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
