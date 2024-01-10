/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.api.action;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestController;

import java.io.IOException;
import java.util.Objects;

public class APIResponse extends ActionResponse implements ToXContentObject {

    private Version version;
    private Build build;
    private RestController restController;

    APIResponse() {

    }

    APIResponse(StreamInput in) throws IOException {
        super(in);
        version = in.readVersion();
        build = in.readBuild();
    }

    APIResponse(Version version, Build build) {
        this.version = version;
        this.build = build;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVersion(version);
        out.writeBuild(build);
    }

    public Version getVersion() {
        return version;
    }

    public Build getBuild() {
        return build;
    }

    public void setRestController(RestController restController) {
        this.restController = restController;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
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

        builder.endObject();
        return builder;
    }

    private static final ObjectParser<APIResponse, Void> PARSER = new ObjectParser<>(APIResponse.class.getName(), true, APIResponse::new);

    static {
        PARSER.declareObject((response, value) -> {
            final String buildType = (String) value.get("build_type");
            response.build = new Build(
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

    public static APIResponse fromXContent(XContentParser parser) {
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
        APIResponse other = (APIResponse) o;
        return Objects.equals(version, other.version) && Objects.equals(build, other.build);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, build);
    }

    @Override
    public String toString() {
        return "APIResponse{" + '\'' + ", version=" + version + '\'' + ", build=" + build + '}';
    }
}
