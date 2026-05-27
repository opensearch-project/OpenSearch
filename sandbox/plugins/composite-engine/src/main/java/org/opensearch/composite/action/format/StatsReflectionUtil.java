/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.plugin.stats.DataFormatShardStats;

import java.lang.reflect.Method;

/**
 * Utility for extracting {@link DataFormatShardStats} from engine instances via reflection,
 * avoiding compile-time dependencies on concrete plugin types across classloader boundaries.
 *
 * @opensearch.experimental
 */
public final class StatsReflectionUtil {

    private static final Logger logger = LogManager.getLogger(StatsReflectionUtil.class);

    /** Empty sentinel stats for shards that have no matching engine. */
    public static final org.opensearch.core.xcontent.ToXContentFragment EMPTY_STATS = (b, p) -> b;

    private StatsReflectionUtil() {}

    /**
     * Reflectively calls add(other) on a stats object for cross-shard aggregation.
     * Both ParquetShardStats and LuceneShardStats expose {@code add(SameType)} returning a new aggregate.
     * Returns null if reflection fails (with WARN log).
     */
    public static org.opensearch.core.xcontent.ToXContentFragment invokeAdd(
        org.opensearch.core.xcontent.ToXContentFragment base,
        org.opensearch.core.xcontent.ToXContentFragment other
    ) {
        try {
            Method m = base.getClass().getMethod("add", base.getClass());
            Object result = m.invoke(base, other);
            if (result instanceof org.opensearch.core.xcontent.ToXContentFragment x) return x;
        } catch (Exception e) {
            logger.warn("Failed to invoke add() reflectively on [{}]: {}", base.getClass().getName(), e.getMessage(), e);
        }
        return null;
    }

    /**
     * Reflectively calls getStats() on an engine. Handles both direct DataFormatShardStats returns
     * and tracker objects that have a stats() method returning DataFormatShardStats.
     *
     * <p>Returns the result as {@link org.opensearch.core.xcontent.ToXContentFragment} (loaded by the
     * bootstrap classloader, shared across plugins) rather than {@link DataFormatShardStats}, because
     * each plugin bundles its own copy of plugin-stats-spi and `instanceof DataFormatShardStats`
     * checks fail across classloader boundaries.
     */
    public static org.opensearch.core.xcontent.ToXContentFragment invokeGetStats(Object engine) {
        try {
            Method m = engine.getClass().getMethod("getStats");
            Object result = m.invoke(engine);
            if (result instanceof org.opensearch.core.xcontent.ToXContentFragment x) {
                return x;
            }
            // Handle tracker pattern: getStats() returns a tracker with stats() method
            if (result != null) {
                try {
                    Method statsM = result.getClass().getMethod("stats");
                    Object snapshot = statsM.invoke(result);
                    if (snapshot instanceof org.opensearch.core.xcontent.ToXContentFragment x2) {
                        return x2;
                    }
                } catch (NoSuchMethodException ignored) {
                    // not a tracker
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to invoke getStats() reflectively on engine [{}]: {}", engine.getClass().getName(), e.getMessage(), e);
        }
        return null;
    }
}
