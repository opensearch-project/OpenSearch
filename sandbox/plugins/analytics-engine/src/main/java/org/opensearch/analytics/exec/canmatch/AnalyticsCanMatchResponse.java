/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

/**
 * Response from a can-match check. Contains:
 * <ul>
 *   <li>{@code canMatch} — whether any row group in this shard can match the filter</li>
 *   <li>{@code minValue} / {@code maxValue} — optional sort-key bounds for sort optimization
 *       (coordinator can order shard dispatch by min-value for early termination)</li>
 * </ul>
 */
public class AnalyticsCanMatchResponse extends TransportResponse {

    private final boolean canMatch;
    private final long minValue;
    private final long maxValue;

    public static final AnalyticsCanMatchResponse YES = new AnalyticsCanMatchResponse(true, Long.MIN_VALUE, Long.MAX_VALUE);
    public static final AnalyticsCanMatchResponse NO = new AnalyticsCanMatchResponse(false, 0, 0);

    public AnalyticsCanMatchResponse(boolean canMatch, long minValue, long maxValue) {
        this.canMatch = canMatch;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public AnalyticsCanMatchResponse(StreamInput in) throws IOException {
        this.canMatch = in.readBoolean();
        this.minValue = in.readLong();
        this.maxValue = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(canMatch);
        out.writeLong(minValue);
        out.writeLong(maxValue);
    }

    public boolean canMatch() {
        return canMatch;
    }

    public long getMinValue() {
        return minValue;
    }

    public long getMaxValue() {
        return maxValue;
    }
}
