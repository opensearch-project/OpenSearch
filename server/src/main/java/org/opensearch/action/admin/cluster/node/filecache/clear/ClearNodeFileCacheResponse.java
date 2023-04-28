/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.filecache.clear;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * The response of a clear filecache action for a single node.
 *
 * @opensearch.internal
 */
public class ClearNodeFileCacheResponse extends BaseNodeResponse implements ToXContentFragment {

    boolean cleared;

    long count;

    protected ClearNodeFileCacheResponse(StreamInput in) throws IOException {
        super(in);
        cleared = in.readBoolean();
        count = in.readLong();
    }

    public ClearNodeFileCacheResponse(DiscoveryNode discoveryNode, boolean cleared, long count) {
        super(discoveryNode);
        this.cleared = cleared;
        this.count = count;
    }

    public boolean isCleared() {
        return cleared;
    }

    public long getCount() {
        return count;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(cleared);
        out.writeLong(count);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("name", getNode().getName());

        builder.startArray("roles");
        for (DiscoveryNodeRole role : getNode().getRoles()) {
            builder.value(role.roleName());
        }
        builder.endArray();

        builder.field("cleared", cleared);
        builder.field("item_count", count);
        return builder;
    }
}
