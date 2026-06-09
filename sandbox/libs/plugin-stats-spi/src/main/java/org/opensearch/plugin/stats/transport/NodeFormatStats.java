/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats.transport;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.stats.DataFormatShardStats;

import java.io.IOException;

/**
 * Per-node response holding typed format stats.
 *
 * @param <T> concrete shard-stats type
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class NodeFormatStats<T extends DataFormatShardStats<T>> extends BaseNodeResponse implements ToXContentFragment {

    private final String formatName;
    private final T stats;

    public NodeFormatStats(DiscoveryNode node, String formatName, T stats) {
        super(node);
        this.formatName = formatName;
        this.stats = stats;
    }

    public NodeFormatStats(StreamInput in, Writeable.Reader<T> reader) throws IOException {
        super(in);
        this.formatName = in.readString();
        if (in.readBoolean()) {
            this.stats = reader.read(in);
        } else {
            this.stats = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(formatName);
        if (stats != null) {
            out.writeBoolean(true);
            stats.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public String formatName() {
        return formatName;
    }

    public T stats() {
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
