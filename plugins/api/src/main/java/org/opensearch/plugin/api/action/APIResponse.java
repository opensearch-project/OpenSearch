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
import org.opensearch.action.main.MainResponse;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
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

        builder.field("openapi", "3.0.1");

        builder.startObject("info")
            .field("title", build.getDistribution())
            .field("description", MainResponse.TAGLINE)
            .field("version", build.getQualifiedVersion())
            .endObject();

        restController.toXContent(builder, params);

        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "APIResponse{" + '\'' + ", version=" + version + '\'' + ", build=" + build + '}';
    }
}
