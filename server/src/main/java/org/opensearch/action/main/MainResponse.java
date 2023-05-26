/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.main;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * The main response of opensearch
 *
 * @opensearch.internal
 */
public class MainResponse extends ActionResponse implements ToXContentObject {

    private String nodeName;
    private Version version;
    private ClusterName clusterName;
    private String clusterUuid;
    private Build build;
    public static final String TAGLINE = "The OpenSearch Project: https://opensearch.org/";

    MainResponse() {}

    MainResponse(StreamInput in) throws IOException {
        super(in);
        nodeName = in.readString();
        version = in.readVersion();
        clusterName = new ClusterName(in);
        clusterUuid = in.readString();
        build = in.readBuild();
    }

    public MainResponse(String nodeName, Version version, ClusterName clusterName, String clusterUuid, Build build) {
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
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeName);
        out.writeVersion(version);
        clusterName.writeTo(out);
        out.writeString(clusterUuid);
        out.writeBuild(build);
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

    private static final ObjectParser<MainResponse, Void> PARSER = new ObjectParser<>(
        MainResponse.class.getName(),
        true,
        MainResponse::new
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

    public static MainResponse fromXContent(XContentParser parser) {
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
        MainResponse other = (MainResponse) o;
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
        return "MainResponse{"
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
