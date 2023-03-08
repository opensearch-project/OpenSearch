
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Inner node get all pits response
 */
public class GetAllPitNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    /**
     * List of active PITs in the associated node
     */
    private final List<ListPitInfo> pitInfos;

    public GetAllPitNodeResponse(DiscoveryNode node, List<ListPitInfo> pitInfos) {
        super(node);
        if (pitInfos == null) {
            throw new IllegalArgumentException("Pits info cannot be null");
        }
        this.pitInfos = Collections.unmodifiableList(pitInfos);
    }

    public GetAllPitNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.pitInfos = Collections.unmodifiableList(in.readList(ListPitInfo::new));
    }

    public List<ListPitInfo> getPitInfos() {
        return pitInfos;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(pitInfos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("node", this.getNode().getName());
        builder.startArray("pitInfos");
        for (ListPitInfo pit : pitInfos) {
            pit.toXContent(builder, params);
        }

        builder.endArray();
        builder.endObject();
        return builder;
    }
}
