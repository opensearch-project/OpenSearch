/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

/**
 * Update PIT context response with creation time, keep alive etc.
 */
public class UpdatePitContextResponse extends TransportResponse {
    private final String pitId;

    private final long creationTime;

    private final long keepAlive;

    UpdatePitContextResponse(StreamInput in) throws IOException {
        super(in);
        pitId = in.readString();
        creationTime = in.readLong();
        keepAlive = in.readLong();
    }

    public UpdatePitContextResponse(String pitId, long creationTime, long keepAlive) {
        this.pitId = pitId;
        this.keepAlive = keepAlive;
        this.creationTime = creationTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pitId);
        out.writeLong(creationTime);
        out.writeLong(keepAlive);
    }

    public String getPitId() {
        return pitId;
    }

    public long getKeepAlive() {
        return keepAlive;
    }

    public long getCreationTime() {
        return creationTime;
    }
}
