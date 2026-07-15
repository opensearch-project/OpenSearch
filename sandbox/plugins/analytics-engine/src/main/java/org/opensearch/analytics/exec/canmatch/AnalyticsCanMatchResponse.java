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
 * Response from a data node indicating whether the shard can possibly
 * match the query's filter predicates.
 *
 * @opensearch.internal
 */
public class AnalyticsCanMatchResponse extends TransportResponse {

    private final boolean canMatch;

    public AnalyticsCanMatchResponse(boolean canMatch) {
        this.canMatch = canMatch;
    }

    public AnalyticsCanMatchResponse(StreamInput in) throws IOException {
        this.canMatch = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(canMatch);
    }

    public boolean canMatch() {
        return canMatch;
    }
}
