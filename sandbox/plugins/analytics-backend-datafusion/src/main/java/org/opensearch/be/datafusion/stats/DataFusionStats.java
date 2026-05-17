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
import org.opensearch.plugin.stats.PluginStats;

import java.io.IOException;
import java.util.Objects;

/**
 * Top-level stats container for the DataFusion backend.
 *
 * <p>Implements {@link PluginStats} for Mustang Stats Framework compatibility,
 * {@link Writeable} for transport serialization, and {@link ToXContentFragment}
 * for JSON rendering.
 *
 * <p>Composes {@link NativeExecutorsStats} rather than duplicating its fields,
 * making it extensible for future metric categories (e.g. MemoryPoolStats).
 * No inner classes — {@code RuntimeMetrics} and {@code TaskMonitorStats} belong
 * to {@link NativeExecutorsStats}.
 */
public class DataFusionStats implements PluginStats, Writeable, ToXContentFragment {

    private final NativeExecutorsStats nativeExecutorsStats; // nullable
    private final PartitionGateStats datanodeGateStats; // nullable
    private final PartitionGateStats coordinatorGateStats; // nullable

    /**
     * Construct from components.
     *
     * @param nativeExecutorsStats  the native executor metrics (nullable)
     * @param datanodeGateStats     the datanode partition gate metrics (nullable)
     * @param coordinatorGateStats  the coordinator partition gate metrics (nullable)
     */
    public DataFusionStats(
        NativeExecutorsStats nativeExecutorsStats,
        PartitionGateStats datanodeGateStats,
        PartitionGateStats coordinatorGateStats
    ) {
        this.nativeExecutorsStats = nativeExecutorsStats;
        this.datanodeGateStats = datanodeGateStats;
        this.coordinatorGateStats = coordinatorGateStats;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public DataFusionStats(StreamInput in) throws IOException {
        this.nativeExecutorsStats = in.readOptionalWriteable(NativeExecutorsStats::new);
        this.datanodeGateStats = in.readOptionalWriteable(PartitionGateStats::new);
        this.coordinatorGateStats = in.readOptionalWriteable(PartitionGateStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(nativeExecutorsStats);
        out.writeOptionalWriteable(datanodeGateStats);
        out.writeOptionalWriteable(coordinatorGateStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (nativeExecutorsStats != null) {
            nativeExecutorsStats.toXContent(builder, params);
        }
        if (datanodeGateStats != null) {
            datanodeGateStats.toXContent(builder, params);
        }
        if (coordinatorGateStats != null) {
            coordinatorGateStats.toXContent(builder, params);
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
     * Returns the datanode partition gate metrics, or {@code null} if absent.
     */
    public PartitionGateStats getDatanodeGateStats() {
        return datanodeGateStats;
    }

    /**
     * Returns the coordinator partition gate metrics, or {@code null} if absent.
     */
    public PartitionGateStats getCoordinatorGateStats() {
        return coordinatorGateStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFusionStats that = (DataFusionStats) o;
        return Objects.equals(nativeExecutorsStats, that.nativeExecutorsStats)
            && Objects.equals(datanodeGateStats, that.datanodeGateStats)
            && Objects.equals(coordinatorGateStats, that.coordinatorGateStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nativeExecutorsStats, datanodeGateStats, coordinatorGateStats);
    }
}
