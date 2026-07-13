/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Top-level stats container for the DataFusion backend.
 *
 * <p>Implements {@link Writeable} for transport serialization and {@link ToXContentFragment}
 * for JSON rendering.
 *
 * <p>Composes {@link NativeExecutorsStats} rather than duplicating its fields,
 * making it extensible for future metric categories (e.g. MemoryPoolStats).
 * No inner classes — {@code RuntimeMetrics} and {@code TaskMonitorStats} belong
 * to {@link NativeExecutorsStats}.
 */
public class DataFusionStats implements Writeable, ToXContentFragment {

    private final NativeExecutorsStats nativeExecutorsStats; // nullable
    private final PartitionGateStats fragmentExecutorGateStats; // nullable
    private final AdaptiveBudgetStats adaptiveBudget; // nullable
    private final SpillStats spillStats; // nullable
    private final CacheStats cacheStats; // nullable
    private final SearchStats searchStats; // nullable
    private final LiquidCacheStats liquidCacheStats; // nullable

    /**
     * Construct from all components.
     *
     * @param nativeExecutorsStats       the native executor metrics (nullable)
     * @param fragmentExecutorGateStats  the fragment executor partition gate metrics (nullable)
     * @param adaptiveBudget             the adaptive budget metrics (nullable)
     * @param spillStats                 the spill directory stats (nullable)
     * @param cacheStats                 the parquet metadata + statistics cache metrics (nullable)
     * @param searchStats                the search execution metrics (nullable)
     * @param liquidCacheStats           the liquid cache metrics (nullable)
     */
    public DataFusionStats(
        NativeExecutorsStats nativeExecutorsStats,
        PartitionGateStats fragmentExecutorGateStats,
        AdaptiveBudgetStats adaptiveBudget,
        SpillStats spillStats,
        CacheStats cacheStats,
        SearchStats searchStats,
        LiquidCacheStats liquidCacheStats
    ) {
        this.nativeExecutorsStats = nativeExecutorsStats;
        this.fragmentExecutorGateStats = fragmentExecutorGateStats;
        this.adaptiveBudget = adaptiveBudget;
        this.spillStats = spillStats;
        this.cacheStats = cacheStats;
        this.searchStats = searchStats;
        this.liquidCacheStats = liquidCacheStats;
    }

    public DataFusionStats(
        NativeExecutorsStats nativeExecutorsStats,
        PartitionGateStats fragmentExecutorGateStats,
        AdaptiveBudgetStats adaptiveBudget,
        SpillStats spillStats,
        CacheStats cacheStats,
        SearchStats searchStats
    ) {
        this(nativeExecutorsStats, fragmentExecutorGateStats, adaptiveBudget, spillStats, cacheStats, searchStats, null);
    }

    public DataFusionStats(
        NativeExecutorsStats nativeExecutorsStats,
        PartitionGateStats fragmentExecutorGateStats,
        AdaptiveBudgetStats adaptiveBudget,
        SpillStats spillStats
    ) {
        this(nativeExecutorsStats, fragmentExecutorGateStats, adaptiveBudget, spillStats, null, null, null);
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public DataFusionStats(StreamInput in) throws IOException {
        this.nativeExecutorsStats = in.readOptionalWriteable(NativeExecutorsStats::new);
        this.fragmentExecutorGateStats = in.readOptionalWriteable(PartitionGateStats::new);
        this.adaptiveBudget = in.readOptionalWriteable(AdaptiveBudgetStats::new);
        this.spillStats = in.readOptionalWriteable(SpillStats::new);
        this.cacheStats = in.readOptionalWriteable(CacheStats::new);
        this.searchStats = in.readOptionalWriteable(SearchStats::new);
        this.liquidCacheStats = in.readOptionalWriteable(LiquidCacheStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(nativeExecutorsStats);
        out.writeOptionalWriteable(fragmentExecutorGateStats);
        out.writeOptionalWriteable(adaptiveBudget);
        out.writeOptionalWriteable(spillStats);
        out.writeOptionalWriteable(cacheStats);
        out.writeOptionalWriteable(searchStats);
        out.writeOptionalWriteable(liquidCacheStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (nativeExecutorsStats != null) {
            nativeExecutorsStats.toXContent(builder, params);
        }
        if (fragmentExecutorGateStats != null) {
            fragmentExecutorGateStats.toXContent(builder, params);
        }
        if (adaptiveBudget != null) {
            adaptiveBudget.toXContent(builder, params);
        }
        if (spillStats != null) {
            spillStats.toXContent(builder, params);
        }
        if (cacheStats != null) {
            cacheStats.toXContent(builder, params);
        }
        if (searchStats != null) {
            searchStats.toXContent(builder, params);
        }
        if (liquidCacheStats != null) {
            liquidCacheStats.toXContent(builder, params);
        }
        return builder;
    }

    /**
     * Returns the native executor metrics, or {@code null} if absent.
     */
    public NativeExecutorsStats getNativeExecutorsStats() {
        return nativeExecutorsStats;
    }

    /**
     * Returns the fragment executor partition gate metrics, or {@code null} if absent.
     */
    public PartitionGateStats getFragmentExecutorGateStats() {
        return fragmentExecutorGateStats;
    }

    /**
     * Returns the adaptive budget metrics, or {@code null} if absent.
     */
    public AdaptiveBudgetStats getAdaptiveBudgetStats() {
        return adaptiveBudget;
    }

    /**
     * Returns the spill directory metrics, or {@code null} if absent.
     */
    public SpillStats getSpillStats() {
        return spillStats;
    }

    /**
     * Returns the parquet cache metrics, or {@code null} if absent.
     */
    public CacheStats getCacheStats() {
        return cacheStats;
    }

    /**
     * Returns the search execution metrics, or {@code null} if absent.
     */
    public SearchStats getSearchStats() {
        return searchStats;
    }

    /**
     * Returns the liquid cache metrics, or {@code null} if absent.
     */
    public LiquidCacheStats getLiquidCacheStats() {
        return liquidCacheStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFusionStats that = (DataFusionStats) o;
        return Objects.equals(nativeExecutorsStats, that.nativeExecutorsStats)
            && Objects.equals(fragmentExecutorGateStats, that.fragmentExecutorGateStats)
            && Objects.equals(adaptiveBudget, that.adaptiveBudget)
            && Objects.equals(spillStats, that.spillStats)
            && Objects.equals(cacheStats, that.cacheStats)
            && Objects.equals(searchStats, that.searchStats)
            && Objects.equals(liquidCacheStats, that.liquidCacheStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            nativeExecutorsStats,
            fragmentExecutorGateStats,
            adaptiveBudget,
            spillStats,
            cacheStats,
            searchStats,
            liquidCacheStats
        );
    }
}
