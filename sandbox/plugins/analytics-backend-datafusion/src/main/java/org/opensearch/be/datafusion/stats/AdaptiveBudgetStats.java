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
 * Stats for the adaptive memory budget admission layer.
 *
 * <p>Tracks two cumulative counters:
 * <ul>
 *   <li>{@code fallbacks} — times {@code target_partitions} was halved during retry</li>
 *   <li>{@code rejections} — times budget acquisition returned an error</li>
 * </ul>
 *
 * <p>Implements {@link Writeable} for transport serialization and
 * {@link ToXContentFragment} for JSON rendering under the key {@code "adaptive_budget"}.
 */
public class AdaptiveBudgetStats implements Writeable, ToXContentFragment {

    /** Cumulative count of fallback reductions (target_partitions halved). */
    public final long fallbacks;

    /** Cumulative count of budget acquisition rejections. */
    public final long rejections;

    /**
     * Construct from explicit field values.
     *
     * @param fallbacks  total fallback reductions
     * @param rejections total budget rejections
     */
    public AdaptiveBudgetStats(long fallbacks, long rejections) {
        this.fallbacks = fallbacks;
        this.rejections = rejections;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public AdaptiveBudgetStats(StreamInput in) throws IOException {
        this.fallbacks = in.readVLong();
        this.rejections = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fallbacks);
        out.writeVLong(rejections);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("adaptive_budget");
        builder.field("fallbacks", fallbacks);
        builder.field("rejections", rejections);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AdaptiveBudgetStats that = (AdaptiveBudgetStats) o;
        return fallbacks == that.fallbacks && rejections == that.rejections;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fallbacks, rejections);
    }
}
