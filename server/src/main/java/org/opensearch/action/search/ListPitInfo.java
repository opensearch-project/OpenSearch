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
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * This holds information about pit reader context such as pit id and creation time
 */
public class ListPitInfo implements ToXContentFragment, Writeable {
    private final String pitId;
    private final long creationTime;
    private final long keepAlive;

    public ListPitInfo(String pitId, long creationTime, long keepAlive) {
        this.pitId = pitId;
        this.creationTime = creationTime;
        this.keepAlive = keepAlive;
    }

    public ListPitInfo(StreamInput in) throws IOException {
        this.pitId = in.readString();
        this.creationTime = in.readLong();
        this.keepAlive = in.readLong();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("pitId", pitId);
        builder.field("creationTime", creationTime);
        builder.field("keepAlive", keepAlive);
        builder.endObject();
        return builder;
    }

    public String getPitId() {
        return pitId;
    }

    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pitId);
        out.writeLong(creationTime);
        out.writeLong(keepAlive);
    }
}
