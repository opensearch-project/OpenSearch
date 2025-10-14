/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.cache;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Response for pruning remote file cache operation from a single node.
 * Contains essential metrics about the cache operation including freed bytes
 * and cache capacity.
 *
 * @opensearch.internal
 */
public class NodePruneCacheResponse extends BaseNodeResponse {

    private final long prunedBytes;
    private final long cacheCapacity;

    public NodePruneCacheResponse(StreamInput in) throws IOException {
        super(in);
        this.prunedBytes = in.readLong();
        this.cacheCapacity = in.readLong();
    }

    public NodePruneCacheResponse(DiscoveryNode node, long prunedBytes, long cacheCapacity) {
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
        if ((o instanceof NodePruneCacheResponse) == false) return false;
        if (!super.equals(o)) return false;
        NodePruneCacheResponse that = (NodePruneCacheResponse) o;
        return prunedBytes == that.prunedBytes && cacheCapacity == that.cacheCapacity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prunedBytes, cacheCapacity);
    }

    @Override
    public String toString() {
        return "NodePruneCacheResponse{"
            + "node="
            + getNode().getId()
            + ", prunedBytes="
            + prunedBytes
            + ", cacheCapacity="
            + cacheCapacity
            + '}';
    }
}
