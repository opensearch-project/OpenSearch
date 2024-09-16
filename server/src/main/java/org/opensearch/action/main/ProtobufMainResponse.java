/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.main;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * The main response of opensearch
*
* @opensearch.internal
*/
public class ProtobufMainResponse extends ProtobufActionResponse implements ToXContentObject {

    private String nodeName;
    private Version version;
    private ClusterName clusterName;
    private String clusterUuid;
    private Build build;
    public static final String TAGLINE = "The OpenSearch Project: https://opensearch.org/";

    ProtobufMainResponse() {}

    ProtobufMainResponse(byte[] in) throws IOException {
        super(in);
    }

    public ProtobufMainResponse(String nodeName, Version version, ClusterName clusterName, String clusterUuid, Build build) {
        this.nodeName = nodeName;
        this.version = version;
        this.clusterName = clusterName;
        this.clusterUuid = clusterUuid;
        this.build = build;
    }

    public String getNodeName() {
        return nodeName;
    }

    public Version getVersion() {
        return version;
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public String getClusterUuid() {
        return clusterUuid;
    }

    public Build getBuild() {
        return build;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", nodeName);
        builder.field("cluster_name", clusterName.value());
        builder.field("cluster_uuid", clusterUuid);
        builder.startObject("version")
            .field("distribution", build.getDistribution())
            .field("number", build.getQualifiedVersion())
            .field("build_type", build.type().displayName())
            .field("build_hash", build.hash())
            .field("build_date", build.date())
            .field("build_snapshot", build.isSnapshot())
            .field("lucene_version", version.luceneVersion.toString())
            .field("minimum_wire_compatibility_version", version.minimumCompatibilityVersion().toString())
            .field("minimum_index_compatibility_version", version.minimumIndexCompatibilityVersion().toString())
            .endObject();
        builder.field("tagline", TAGLINE);
        builder.endObject();
        return builder;
    }

    private static final ObjectParser<ProtobufMainResponse, Void> PARSER = new ObjectParser<>(
        ProtobufMainResponse.class.getName(),
        true,
        ProtobufMainResponse::new
    );

    static {
        PARSER.declareString((response, value) -> response.nodeName = value, new ParseField("name"));
        PARSER.declareString((response, value) -> response.clusterName = new ClusterName(value), new ParseField("cluster_name"));
        PARSER.declareString((response, value) -> response.clusterUuid = value, new ParseField("cluster_uuid"));
        PARSER.declareString((response, value) -> {}, new ParseField("tagline"));
        PARSER.declareObject((response, value) -> {
            final String buildType = (String) value.get("build_type");
            response.build = new Build(
                /*
                * Be lenient when reading on the wire, the enumeration values from other versions might be different than what
                * we know.
                */
                buildType == null ? Build.Type.UNKNOWN : Build.Type.fromDisplayName(buildType, false),
                (String) value.get("build_hash"),
                (String) value.get("build_date"),
                (boolean) value.get("build_snapshot"),
                (String) value.get("number"),
                (String) value.get("distribution")
            );
            response.version = Version.fromString(
                ((String) value.get("number")).replace("-SNAPSHOT", "").replaceFirst("-(alpha\\d+|beta\\d+|rc\\d+)", "")
            );
        }, (parser, context) -> parser.map(), new ParseField("version"));
    }

    public static ProtobufMainResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProtobufMainResponse other = (ProtobufMainResponse) o;
        return Objects.equals(nodeName, other.nodeName)
            && Objects.equals(version, other.version)
            && Objects.equals(clusterUuid, other.clusterUuid)
            && Objects.equals(build, other.build)
            && Objects.equals(clusterName, other.clusterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeName, version, clusterUuid, build, clusterName);
    }

    @Override
    public String toString() {
        return "ProtobufMainResponse{"
            + "nodeName='"
            + nodeName
            + '\''
            + ", version="
            + version
            + ", clusterName="
            + clusterName
            + ", clusterUuid='"
            + clusterUuid
            + '\''
            + ", build="
            + build
            + '}';
    }
}
