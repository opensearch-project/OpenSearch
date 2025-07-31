/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.action;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Information about DataFusion on a specific node
 */
public class NodeDataFusionInfo extends BaseNodeResponse implements ToXContentFragment {

    private final String dataFusionVersion;

    /**
     * Constructor for NodeDataFusionInfo.
     * @param node The discovery node.
     * @param dataFusionVersion The DataFusion version.
     */
    public NodeDataFusionInfo(
        DiscoveryNode node,
        String dataFusionVersion
    ) {
        super(node);
        this.dataFusionVersion = dataFusionVersion;
    }

    /**
     * Constructor for NodeDataFusionInfo from stream input.
     * @param in The stream input.
     * @throws IOException If an I/O error occurs.
     */
    public NodeDataFusionInfo(StreamInput in) throws IOException {
        super(in);
        this.dataFusionVersion = in.readString();
    }

    /**
     * Writes the node info to the stream output.
     * @param out The stream output.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(dataFusionVersion);
    }

    /**
     * Converts the node info to XContent.
     * @param builder The XContent builder.
     * @param params The parameters.
     * @return The XContent builder.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("data_fusion_info");
        builder.field("datafusion_version", dataFusionVersion);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    /**
     * Gets the DataFusion version.
     * @return The DataFusion version.
     */
    public String getDataFusionVersion() {
        return dataFusionVersion;
    }
}
