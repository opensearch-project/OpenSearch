/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.nodes;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Per-node request for streaming data
 */
/**
 * Example class
 */
/** Example */
public class NodeStreamDataRequest extends TransportRequest {
    private int count;
    private long delayMs;

    /**
     * Constructor from stream input
     * @param in stream input
     * @throws IOException if read fails
     */
    /**
     * Constructor
     */
    /** Method */
    public NodeStreamDataRequest(StreamInput in) throws IOException {
        super(in);
        count = in.readInt();
        delayMs = in.readLong();
    }

    /**
     * Constructor from nodes request
     * @param request nodes request
     */
    /**
     * Constructor
     */
    /** Method */
    public NodeStreamDataRequest(StreamNodesDataRequest request) {
        this.count = request.getCount();
        this.delayMs = request.getDelayMs();
    }

    @Override
    /**
     * Method
     * @return result
     */
    /** Method */
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(count);
        out.writeLong(delayMs);
    }

    /**
     * Get count
     * @return count
     */
    /**
     * Method
     * @return result
     */
    /** Method */
    public int getCount() {
        return count;
    }

    /**
     * Get delay in milliseconds
     * @return delay
     */
    /**
     * Method
     * @return result
     */
    /** Method */
    public long getDelayMs() {
        return delayMs;
    }
}
