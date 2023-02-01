/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.configuration;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class ConfigUpdateResponse extends BaseNodesResponse<ConfigUpdateNodeResponse> implements ToXContentObject {

    public ConfigUpdateResponse(StreamInput in) throws IOException {
        super(in);
    }

    public ConfigUpdateResponse(final ClusterName clusterName, List<ConfigUpdateNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    public List<ConfigUpdateNodeResponse> readNodesFrom(final StreamInput in) throws IOException {
        return in.readList(ConfigUpdateNodeResponse::readNodeResponse);
    }

    @Override
    public void writeNodesTo(final StreamOutput out, List<ConfigUpdateNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("configupdate_response");
        builder.field("nodes", getNodesMap());
        builder.field("node_size", getNodes().size());
        builder.field("has_failures", hasFailures());
        builder.field("failures_size", failures().size());
        builder.endObject();

        return builder;
    }
}
