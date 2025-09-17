/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.cache;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Response for pruning remote file cache
 *
 * @opensearch.internal
 */
public class PruneCacheResponse extends ActionResponse implements ToXContentObject {

    private final boolean acknowledged;
    private final long prunedBytes;

    /**
     * Constructor for successful response
     *
     * @param acknowledged whether the operation was acknowledged
     * @param prunedBytes  the number of bytes freed by the prune operation
     */
    public PruneCacheResponse(boolean acknowledged, long prunedBytes) {
        this.acknowledged = acknowledged;
        this.prunedBytes = prunedBytes;
    }

    /**
     * Constructor for stream input
     *
     * @param in the stream input
     * @throws IOException if an I/O exception occurs
     */
    public PruneCacheResponse(StreamInput in) throws IOException {
        super(in);
        this.acknowledged = in.readBoolean();
        this.prunedBytes = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
        out.writeLong(prunedBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("acknowledged", acknowledged);
        builder.field("pruned_bytes", prunedBytes);
        builder.endObject();
        return builder;
    }

    /**
     * @return whether the operation was acknowledged
     */
    public boolean isAcknowledged() {
        return acknowledged;
    }

    /**
     * @return the number of bytes freed by the prune operation
     */
    public long getPrunedBytes() {
        return prunedBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PruneCacheResponse that = (PruneCacheResponse) o;
        return acknowledged == that.acknowledged && prunedBytes == that.prunedBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledged, prunedBytes);
    }

    @Override
    public String toString() {
        return "PruneCacheResponse{" + "acknowledged=" + acknowledged + ", prunedBytes=" + prunedBytes + '}';
    }
}
