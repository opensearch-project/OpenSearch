/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer.action;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.blockcache.foyer.FoyerAggregatedStats;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Per-node response for {@code GET /_nodes/foyer_cache/stats}.
 *
 * <p>{@code stats} is {@code null} when this node does not have a Foyer block
 * cache registered (e.g. cold/hot node, or cache disabled by settings). The
 * REST handler skips nodes with a {@code null} payload.
 *
 * @opensearch.internal
 */
public class FoyerCacheNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    /**
     * Rich Foyer stats snapshot for this node, or {@code null} if the Foyer
     * block cache is not active on this node.
     */
    @Nullable
    private final FoyerAggregatedStats stats;

    public FoyerCacheNodeResponse(DiscoveryNode node, @Nullable FoyerAggregatedStats stats) {
        super(node);
        this.stats = stats;
    }

    public FoyerCacheNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.stats = in.readBoolean() ? new FoyerAggregatedStats(in) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (stats != null) {
            out.writeBoolean(true);
            stats.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    /** Returns the stats snapshot for this node, or {@code null} if unavailable. */
    @Nullable
    public FoyerAggregatedStats getStats() {
        return stats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (stats != null) {
            stats.toXContent(builder, params);
        }
        return builder;
    }
}
