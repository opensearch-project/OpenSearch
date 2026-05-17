/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.deployment;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractNamedDiffable;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Cluster metadata that stores the set of active deployments.
 *
 * @opensearch.internal
 */
public class DeploymentMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "deployment";

    private final Map<String, Deployment> deployments;

    public DeploymentMetadata(Map<String, Deployment> deployments) {
        this.deployments = Collections.unmodifiableMap(Objects.requireNonNull(deployments));
    }

    public DeploymentMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, Deployment> deployments = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String id = in.readString();
            Deployment deployment = new Deployment(in);
            deployments.put(id, deployment);
        }
        this.deployments = Collections.unmodifiableMap(deployments);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(deployments.size());
        for (Map.Entry<String, Deployment> entry : deployments.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    public Map<String, Deployment> getDeployments() {
        return deployments;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("deployments");
        for (Map.Entry<String, Deployment> entry : deployments.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public static DeploymentMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, Deployment> deployments = new HashMap<>();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("deployments".equals(currentFieldName)) {
                    parseDeployments(parser, deployments);
                }
            }
        }
        return new DeploymentMetadata(deployments);
    }

    private static void parseDeployments(XContentParser parser, Map<String, Deployment> deployments) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String deploymentId = parser.currentName();
                parser.nextToken();
                DeploymentState state = null;
                Map<String, String> attributes = null;
                String innerField;
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    innerField = parser.currentName();
                    parser.nextToken();
                    if ("state".equals(innerField)) {
                        state = DeploymentState.fromString(parser.text());
                    } else if ("attributes".equals(innerField)) {
                        attributes = parser.mapStrings();
                    }
                }
                if (state != null && attributes != null) {
                    deployments.put(deploymentId, new Deployment(state, attributes));
                }
            }
        }
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_3_7_0;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.API_AND_GATEWAY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeploymentMetadata that = (DeploymentMetadata) o;
        return deployments.equals(that.deployments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deployments);
    }
}
