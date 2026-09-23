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
 * Acknowledgement for {@link AnalyticsClearShuffleRequest}. Carries the number of buffers removed
 * (for logging / diagnostics); the coordinator ignores it (fire-and-forget cleanup).
 *
 * @opensearch.internal
 */
public class AnalyticsClearShuffleResponse extends ActionResponse {

    private final int removed;

    public AnalyticsClearShuffleResponse(int removed) {
        this.removed = removed;
    }

    public AnalyticsClearShuffleResponse(StreamInput in) throws IOException {
        this.removed = in.readVInt();
    }

    public int getRemoved() {
        return removed;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(removed);
    }
}
