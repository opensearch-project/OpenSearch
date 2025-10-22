/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.filecache;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Response for pruning remote file cache operation from a single node.
 * Contains essential metrics about the cache operation including freed bytes
 * and cache capacity.
 *
 * @opensearch.internal
 */
public class NodePruneFileCacheResponse extends BaseNodeResponse implements ToXContentFragment {

    private final long prunedBytes;
    private final long cacheCapacity;

    public NodePruneFileCacheResponse(StreamInput in) throws IOException {
        super(in);
        this.prunedBytes = in.readLong();
        this.cacheCapacity = in.readLong();
    }

    public NodePruneFileCacheResponse(DiscoveryNode node, long prunedBytes, long cacheCapacity) {
        super(node);
        this.prunedBytes = prunedBytes;
        this.cacheCapacity = cacheCapacity;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(prunedBytes);
        out.writeLong(cacheCapacity);
    }

    public long getPrunedBytes() {
        return prunedBytes;
    }

    public long getCacheCapacity() {
        return cacheCapacity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof NodePruneFileCacheResponse) == false) return false;
        if (!super.equals(o)) return false;
        NodePruneFileCacheResponse that = (NodePruneFileCacheResponse) o;
        return prunedBytes == that.prunedBytes && cacheCapacity == that.cacheCapacity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prunedBytes, cacheCapacity);
    }

    @Override
    public String toString() {
        return "NodePruneFileCacheResponse{"
            + "node="
            + getNode().getId()
            + ", prunedBytes="
            + prunedBytes
            + ", cacheCapacity="
            + cacheCapacity
            + '}';
    }

    /**
     * Serializes this node response to XContent format.
     * Outputs node identification fields and cache operation metrics.
     *
     * @param builder the XContent builder
     * @param params serialization parameters
     * @return the XContent builder for method chaining
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("name", getNode().getName());
        builder.field("transport_address", getNode().getAddress().toString());
        builder.field("host", getNode().getHostName());
        builder.field("ip", getNode().getHostAddress());
        builder.field("pruned_bytes", prunedBytes);
        builder.field("cache_capacity", cacheCapacity);
        return builder;
    }
}
