/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.cache;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.node.resource.tracker.ResourceTrackerSettings;


/**
 * Settings for the DataFusion parquet caches.
 *
 * <h2>Budget model</h2>
 *
 * <pre>
 * node.native_memory.limit = 80% of off-heap
 *   ├── 71% → datafusion.memory_pool_limit_bytes  (operator pool)
 *   ├──  3% → datafusion.metadata_index_cache.total_size   (all metadata caches (footer + page indexes), this class)
 *   │     ├── 50% → metadata cache   (footer metadata, Rust jemalloc)
 *   │     ├── 35% → offset index     (projection-driven, Rust jemalloc)
 *   │     └── 15% → column index     (predicate-driven, Rust jemalloc)
 *   ├──  8% → Arrow ingest pool
 *   ├──  5% → Arrow flight pool
 *   ├──  5% → Arrow query pool
 *   ├──  5% → Parquet write pool
 *   └──  3% → Parquet merge pool
 *   = 100%
 * </pre>
 *
 * <p>The three sub-cache percentages must sum to &lt;= 100 (unused headroom is accepted). Changing any one without adjusting
 * the others is rejected at validation time.
 *
 * <p>The statistics cache is sized independently (not part of the 3% total) because it
 * holds file-level row-group statistics, not page-level indexes — a different working set
 * with different eviction characteristics.
 */
public class CacheSettings {

    public static final String METADATA_CACHE_ENABLED_KEY = "datafusion.metadata.cache.enabled";
    public static final Setting<Boolean> METADATA_CACHE_ENABLED = Setting.boolSetting(
        METADATA_CACHE_ENABLED_KEY,
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String STATISTICS_CACHE_ENABLED_KEY = "datafusion.statistics.cache.enabled";
    public static final Setting<Boolean> STATISTICS_CACHE_ENABLED = Setting.boolSetting(
        STATISTICS_CACHE_ENABLED_KEY,
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Eviction policy is no longer configurable: every DataFusion cache uses S3-FIFO
    // (scan-resistant), fixed in native code. The former
    // datafusion.{metadata,statistics,column_index,offset_index}.cache.eviction.type settings
    // have been removed.

    // Page-cache total budget (3% of node.native_memory.limit)

    public static final String METADATA_INDEX_CACHE_TOTAL_SIZE_KEY = "datafusion.metadata_index_cache.total_size";

    /**
     * Total byte budget for all three metadata caches (footer metadata, ColumnIndex,
     * OffsetIndex). Defaults to 3% of {@code node.native_memory.limit}; falls back to
     * 500 MB when AC is unconfigured.
     */
    public static final Setting<ByteSizeValue> METADATA_INDEX_CACHE_TOTAL_SIZE = new Setting<>(
        METADATA_INDEX_CACHE_TOTAL_SIZE_KEY,
        CacheSettings::deriveMetadataIndexCacheTotalDefault,
        s -> ByteSizeValue.parseBytesSizeValue(s, new ByteSizeValue(0, ByteSizeUnit.BYTES), METADATA_INDEX_CACHE_TOTAL_SIZE_KEY),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // ── Sub-cache percentages (must sum to <= 100) ───────────────────────────────

    public static final String FOOTER_METADATA_CACHE_PERCENT_KEY = "datafusion.cache.footer_metadata_percent";
    public static final String OFFSET_INDEX_CACHE_PERCENT_KEY = "datafusion.cache.offset_index_percent";
    public static final String COLUMN_INDEX_CACHE_PERCENT_KEY = "datafusion.cache.column_index_percent";
    public static final String STATISTICS_CACHE_PERCENT_KEY = "datafusion.cache.statistics_percent";

    /** Percentage of {@link #METADATA_INDEX_CACHE_TOTAL_SIZE} allocated to the footer metadata cache. Default 48%. */
    public static final Setting<Integer> FOOTER_METADATA_CACHE_PERCENT = Setting.intSetting(
        FOOTER_METADATA_CACHE_PERCENT_KEY,
        48,
        1,
        98,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Percentage of {@link #METADATA_INDEX_CACHE_TOTAL_SIZE} allocated to the OffsetIndex cache.
     * Larger than ColumnIndex because it covers predicate ∪ projection ∪ {col 0}.
     * Default 34%.
     */
    public static final Setting<Integer> OFFSET_INDEX_CACHE_PERCENT = Setting.intSetting(
        OFFSET_INDEX_CACHE_PERCENT_KEY,
        34,
        1,
        98,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** Percentage of {@link #METADATA_INDEX_CACHE_TOTAL_SIZE} allocated to the ColumnIndex cache. Default 13%. */
    public static final Setting<Integer> COLUMN_INDEX_CACHE_PERCENT = Setting.intSetting(
        COLUMN_INDEX_CACHE_PERCENT_KEY,
        13,
        1,
        98,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Percentage of {@link #METADATA_INDEX_CACHE_TOTAL_SIZE} allocated to the file-level statistics cache
     * (row-group min/max/null-count). Default 5%.
     */
    public static final Setting<Integer> STATISTICS_CACHE_PERCENT = Setting.intSetting(
        STATISTICS_CACHE_PERCENT_KEY,
        5,
        1,
        98,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Validate that the four sub-cache percentages sum to &lt;= 100. Unused headroom (sum &lt; 100) is accepted — mirrors Arrow's pool model where sum(max) &lt;= budget.
     *
     * @param metaPct    footer metadata cache percent
     * @param oiPct      offset index cache percent
     * @param ciPct      column index cache percent
     * @param statsPct   statistics cache percent
     */
    public static void validatePercentSum(int metaPct, int oiPct, int ciPct, int statsPct) {
        if (metaPct + oiPct + ciPct + statsPct > 100) {
            throw new IllegalArgumentException(
                "Datafusion cache percentages must sum to <= 100, got: "
                    + FOOTER_METADATA_CACHE_PERCENT_KEY
                    + "="
                    + metaPct
                    + ", "
                    + OFFSET_INDEX_CACHE_PERCENT_KEY
                    + "="
                    + oiPct
                    + ", "
                    + COLUMN_INDEX_CACHE_PERCENT_KEY
                    + "="
                    + ciPct
                    + ", "
                    + STATISTICS_CACHE_PERCENT_KEY
                    + "="
                    + statsPct
                    + " (sum="
                    + (metaPct + oiPct + ciPct + statsPct)
                    + ")"
            );
        }
    }

    /**
     * Compute absolute cache sizes from percent values and a total budget.
     * Returns {@code long[]{footerMetadataBytes, offsetIndexBytes, columnIndexBytes, statisticsBytes}}.
     * Does NOT validate that percents sum to &lt;= 100 — call {@link #validatePercentSum} first.
     */
    public static long[] computeCacheSizes(int metaPct, int oiPct, int ciPct, int statsPct, long totalBytes) {
        return new long[] { totalBytes * metaPct / 100, totalBytes * oiPct / 100, totalBytes * ciPct / 100, totalBytes * statsPct / 100 };
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Default total page-cache budget: 3% of {@code node.native_memory.limit}.
     * Falls back to 500 MB when AC is unconfigured (limit == 0).
     */
    static String deriveMetadataIndexCacheTotalDefault(Settings settings) {
        ByteSizeValue nativeLimit = ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        if (nativeLimit.getBytes() <= 0) {
            return (500L * 1024 * 1024) + "b";
        }
        long total = Math.max(nativeLimit.getBytes() * 3 / 100, 0L);
        return total + "b";
    }
}
