/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response to an {@link AnalyticsShuffleDataRequest}. Carries a {@code backpressureRejected}
 * flag: when {@code true} the buffer was at its per-partition byte cap and the sender should
 * retry with exponential backoff. Ported from OLAP's {@code ShuffleDataResponse}.
 *
 * @opensearch.internal
 */
public class AnalyticsShuffleDataResponse extends ActionResponse {

    private final boolean backpressureRejected;

    public AnalyticsShuffleDataResponse() {
        this(false);
    }

    public AnalyticsShuffleDataResponse(boolean backpressureRejected) {
        this.backpressureRejected = backpressureRejected;
    }

    public AnalyticsShuffleDataResponse(StreamInput in) throws IOException {
        this.backpressureRejected = in.readBoolean();
    }

    public static AnalyticsShuffleDataResponse backpressureReject() {
        return new AnalyticsShuffleDataResponse(true);
    }

    public boolean isBackpressureRejected() {
        return backpressureRejected;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(backpressureRejected);
    }
}
