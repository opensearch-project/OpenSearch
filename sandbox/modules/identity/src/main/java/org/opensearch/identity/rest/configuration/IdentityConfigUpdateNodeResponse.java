/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.configuration;

import java.io.IOException;
import java.util.Arrays;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

/**
 * Response for config update request execution for this node
 */
public class IdentityConfigUpdateNodeResponse extends BaseNodeResponse implements ToXContentObject {

    private String[] updatedConfigTypes;
    private String message;

    public IdentityConfigUpdateNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.updatedConfigTypes = in.readStringArray();
        this.message = in.readOptionalString();
    }

    public IdentityConfigUpdateNodeResponse(final DiscoveryNode node, String[] updatedConfigTypes, String message) {
        super(node);
        this.updatedConfigTypes = updatedConfigTypes;
        this.message = message;
    }

    public static IdentityConfigUpdateNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new IdentityConfigUpdateNodeResponse(in);
    }

    public String[] getUpdatedConfigTypes() {
        return updatedConfigTypes == null ? null : Arrays.copyOf(updatedConfigTypes, updatedConfigTypes.length);
    }

    public String getMessage() {
        return message;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(updatedConfigTypes);
        out.writeOptionalString(message);
    }

    @Override
    public String toString() {
        return "ConfigUpdateNodeResponse [updatedConfigTypes=" + Arrays.toString(updatedConfigTypes) + ", message=" + message + "]";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("updated_config_types", updatedConfigTypes);
        builder.field("updated_config_size", updatedConfigTypes == null ? 0 : updatedConfigTypes.length);
        builder.field("message", message);
        builder.endObject();
        return builder;
    }
}
