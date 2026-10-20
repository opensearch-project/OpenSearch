/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.analytics.spi.ShardSortBounds;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

/**
 * Response from a data node indicating whether the shard can possibly
 * match the query's filter predicates.
 *
 * <p>Also carries the sort column's min/max when the request asked for it. The two are
 * independent: {@code canMatch} drives pruning, {@code bounds} drives shard ordering.
 *
 * @opensearch.internal
 */
public class AnalyticsCanMatchResponse extends TransportResponse {

    private final boolean canMatch;
    @Nullable
    private final ShardSortBounds bounds;

    public AnalyticsCanMatchResponse(boolean canMatch) {
        this(canMatch, null);
    }

    public AnalyticsCanMatchResponse(boolean canMatch, @Nullable ShardSortBounds bounds) {
        this.canMatch = canMatch;
        this.bounds = bounds;
    }

    public AnalyticsCanMatchResponse(StreamInput in) throws IOException {
        this.canMatch = in.readBoolean();
        this.bounds = in.readOptionalWriteable(ShardSortBounds::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(canMatch);
        out.writeOptionalWriteable(bounds);
    }

    public boolean canMatch() {
        return canMatch;
    }

    /**
     * Shard-wide min/max of the requested sort column; {@code null} when none was requested or the
     * shard has no usable statistics for it. Null is a normal answer, not an error — the
     * coordinator sorts those shards last.
     */
    @Nullable
    public ShardSortBounds bounds() {
        return bounds;
    }
}
