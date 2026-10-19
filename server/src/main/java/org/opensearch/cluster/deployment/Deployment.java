/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.deployment;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single deployment operation with a target state and node attributes.
 *
 * @opensearch.internal
 */
public class Deployment implements Writeable, ToXContentObject {

    private final DeploymentState state;
    private final Map<String, String> nodeAttributes;

    public Deployment(DeploymentState state, Map<String, String> nodeAttributes) {
        this.state = Objects.requireNonNull(state);
        this.nodeAttributes = Collections.unmodifiableMap(Objects.requireNonNull(nodeAttributes));
    }

    public Deployment(StreamInput in) throws IOException {
        this.state = DeploymentState.readFrom(in);
        this.nodeAttributes = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        state.writeTo(out);
        out.writeMap(nodeAttributes, StreamOutput::writeString, StreamOutput::writeString);
    }

    public DeploymentState getState() {
        return state;
    }

    public Map<String, String> getNodeAttributes() {
        return nodeAttributes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("state", state.name());
        builder.field("attributes", nodeAttributes);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Deployment that = (Deployment) o;
        return state == that.state && nodeAttributes.equals(that.nodeAttributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, nodeAttributes);
    }
}
