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

public class UpdatePitContextResponse extends TransportResponse {
    private final String pitId;

    private final long createTime;

    private final long keepAlive;

    UpdatePitContextResponse(StreamInput in) throws IOException {
        super(in);
        pitId = in.readString();
        createTime = in.readLong();
        keepAlive = in.readLong();
    }

    public UpdatePitContextResponse(String pitId, long createTime, long keepAlive) {
        this.pitId = pitId;
        this.keepAlive = keepAlive;
        this.createTime = createTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pitId);
        out.writeLong(createTime);
        out.writeLong(keepAlive);
    }

    public String getPitId() {
        return pitId;
    }

    public long getKeepAlive() {
        return keepAlive;
    }

    public long getCreateTime() {
        return createTime;
    }
}
