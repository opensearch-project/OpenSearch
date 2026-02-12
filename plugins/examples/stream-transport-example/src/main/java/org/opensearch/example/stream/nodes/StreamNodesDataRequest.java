/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.nodes;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for streaming data from nodes
 */
public class StreamNodesDataRequest extends BaseNodesRequest<StreamNodesDataRequest> {
    private int count = 5;
    private long delayMs = 500;

    public StreamNodesDataRequest(StreamInput in) throws IOException {
        super(in);
        count = in.readInt();
        delayMs = in.readLong();
    }

    public StreamNodesDataRequest(String... nodesIds) {
        super(nodesIds);
    }

    public StreamNodesDataRequest count(int count) {
        this.count = count;
        return this;
    }

    public StreamNodesDataRequest delayMs(long delayMs) {
        this.delayMs = delayMs;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(count);
        out.writeLong(delayMs);
    }

    public int getCount() {
        return count;
    }

    public long getDelayMs() {
        return delayMs;
    }
}
