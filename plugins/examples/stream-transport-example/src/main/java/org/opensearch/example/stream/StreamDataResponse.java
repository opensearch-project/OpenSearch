/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

class StreamDataResponse extends ActionResponse {
    private final String message;
    private final int sequence;
    private final boolean isLast;

    public StreamDataResponse(String message, int sequence, boolean isLast) {
        this.message = message;
        this.sequence = sequence;
        this.isLast = isLast;
    }

    public StreamDataResponse(StreamInput in) throws IOException {
        super(in);
        message = in.readString();
        sequence = in.readInt();
        isLast = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(message);
        out.writeInt(sequence);
        out.writeBoolean(isLast);
    }

    public String getMessage() {
        return message;
    }

    public int getSequence() {
        return sequence;
    }

    public boolean isLast() {
        return isLast;
    }
}
