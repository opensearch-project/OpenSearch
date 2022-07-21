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
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Inner node get all pits response
 */
public class GetAllPitNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    private List<ListPitInfo> pitsInfo;

    @Inject
    public GetAllPitNodeResponse(StreamInput in, List<ListPitInfo> pitsInfo) throws IOException {
        super(in);
        this.pitsInfo = pitsInfo;
    }

    public GetAllPitNodeResponse(DiscoveryNode node, List<ListPitInfo> pitsInfo) {
        super(node);
        this.pitsInfo = pitsInfo;
    }

    public GetAllPitNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.pitsInfo = Collections.unmodifiableList(in.readList(ListPitInfo::new));
    }

    public List<ListPitInfo> getPitsInfo() {
        return pitsInfo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(pitsInfo);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("node", this.getNode().getName());
        builder.startArray("pitsInfo");
        for (ListPitInfo pit : pitsInfo) {
            pit.toXContent(builder, params);
        }

        builder.endArray();
        builder.endObject();
        return builder;
    }

}
