/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.api;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Represents the response for a node's flight information.
 */
public class NodeFlightInfo extends BaseNodeResponse implements ToXContentObject {
    private final BoundTransportAddress boundAddress;

    /**
     * Constructor for NodeFlightInfo.
     * @param in The stream input to read from.
     * @throws IOException If an I/O error occurs.
     */
    public NodeFlightInfo(StreamInput in) throws IOException {
        super(in);
        boundAddress = new BoundTransportAddress(in);
    }

    /**
     * Constructor for NodeFlightInfo.
     * @param node The discovery node.
     * @param boundAddress The bound transport address.
     */
    public NodeFlightInfo(DiscoveryNode node, BoundTransportAddress boundAddress) {
        super(node);
        this.boundAddress = boundAddress;
    }

    /**
     * Writes the node flight information to the stream.
     * @param out The stream output to write to.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        boundAddress.writeTo(out);
    }

    /**
     * Returns the bound transport address.
     * @return The bound transport address.
     */
    public BoundTransportAddress getBoundAddress() {
        return boundAddress;
    }

    /**
     * Converts the node flight information to XContent.
     * @param builder The XContent builder.
     * @param params  The parameters for the XContent conversion.
     * @return The XContent builder.
     * @throws IOException If an I/O error occurs.
     */
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startObject("flight_server");

        builder.startArray("bound_addresses");
        for (TransportAddress address : boundAddress.boundAddresses()) {
            builder.startObject();
            builder.field("host", address.address().getHostString());
            builder.field("port", address.address().getPort());
            builder.endObject();
        }
        builder.endArray();

        TransportAddress publishAddress = boundAddress.publishAddress();
        builder.startObject("publish_address");
        builder.field("host", publishAddress.address().getHostString());
        builder.field("port", publishAddress.address().getPort());
        builder.endObject();

        builder.endObject();
        builder.endObject();
        return builder;
    }

}
