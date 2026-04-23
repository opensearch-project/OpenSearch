/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.nodes;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Example class
 */
/** Example */
public class NodeStreamDataResponse extends BaseNodeResponse {
    private final String message;
    private final int sequence;

    /**
     * Constructor
     */
    /** Method */
    public NodeStreamDataResponse(StreamInput in) throws IOException {
        super(in);
        message = in.readString();
        sequence = in.readInt();
    }

    /**
     * Constructor
     */
    /** Method */
    public NodeStreamDataResponse(DiscoveryNode node, String message, int sequence) {
        super(node);
        this.message = message;
        this.sequence = sequence;
    }

    @Override
    /**
     * Method
     * @return result
     */
    /** Method */
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(message);
        out.writeInt(sequence);
    }

    /**
     * Method
     * @return result
     */
    /** Method */
    public String getMessage() {
        return message;
    }

    /**
     * Method
     * @return result
     */
    /** Method */
    public int getSequence() {
        return sequence;
    }
}
