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
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

public class UpdatePITContextRequest extends TransportRequest {
    private final String pitId;
    private final long keepAlive;

    private final long createTime;
    private final ShardSearchContextId searchContextId;

    public UpdatePITContextRequest(ShardSearchContextId searchContextId, String pitId, long keepAlive, long createTime) {
        this.pitId = pitId;
        this.searchContextId = searchContextId;
        this.keepAlive = keepAlive;
        this.createTime = createTime;
    }

    UpdatePITContextRequest(StreamInput in) throws IOException {
        super(in);
        pitId = in.readString();
        keepAlive = in.readLong();
        createTime = in.readLong();
        searchContextId = new ShardSearchContextId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(pitId);
        out.writeLong(keepAlive);
        out.writeLong(createTime);
        searchContextId.writeTo(out);
    }

    public ShardSearchContextId getSearchContextId() {
        return searchContextId;
    }

    public String getPitId() {
        return pitId;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getKeepAlive() {
        return keepAlive;
    }
}
