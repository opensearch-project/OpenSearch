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
 * <p>Composes sibling fragments rather than duplicating their fields. New metric
 * categories are added by appending a nullable field, ctor arg, writeTo/readFrom
 * pair, and toXContent call.
 */
public class DataFusionStats implements Writeable, ToXContentFragment {

    private final NativeExecutorsStats nativeExecutorsStats; // nullable
    private final PartitionGateStats datanodeGateStats; // nullable
    private final PartitionGateStats coordinatorGateStats; // nullable
    private final SpillStats spillStats; // nullable

    public DataFusionStats(
        NativeExecutorsStats nativeExecutorsStats,
        PartitionGateStats datanodeGateStats,
        PartitionGateStats coordinatorGateStats,
        SpillStats spillStats
    ) {
        this.nativeExecutorsStats = nativeExecutorsStats;
        this.datanodeGateStats = datanodeGateStats;
        this.coordinatorGateStats = coordinatorGateStats;
        this.spillStats = spillStats;
    }

    public DataFusionStats(StreamInput in) throws IOException {
        this.nativeExecutorsStats = in.readOptionalWriteable(NativeExecutorsStats::new);
        this.datanodeGateStats = in.readOptionalWriteable(PartitionGateStats::new);
        this.coordinatorGateStats = in.readOptionalWriteable(PartitionGateStats::new);
        this.spillStats = in.readOptionalWriteable(SpillStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(nativeExecutorsStats);
        out.writeOptionalWriteable(datanodeGateStats);
        out.writeOptionalWriteable(coordinatorGateStats);
        out.writeOptionalWriteable(spillStats);
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
        if (spillStats != null) {
            spillStats.toXContent(builder, params);
        }
        return builder;
    }

    public NativeExecutorsStats getNativeExecutorsStats() {
        return nativeExecutorsStats;
    }

    public PartitionGateStats getDatanodeGateStats() {
        return datanodeGateStats;
    }

    public PartitionGateStats getCoordinatorGateStats() {
        return coordinatorGateStats;
    }

    public SpillStats getSpillStats() {
        return spillStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFusionStats that = (DataFusionStats) o;
        return Objects.equals(nativeExecutorsStats, that.nativeExecutorsStats)
            && Objects.equals(datanodeGateStats, that.datanodeGateStats)
            && Objects.equals(coordinatorGateStats, that.coordinatorGateStats)
            && Objects.equals(spillStats, that.spillStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nativeExecutorsStats, datanodeGateStats, coordinatorGateStats, spillStats);
    }
}
