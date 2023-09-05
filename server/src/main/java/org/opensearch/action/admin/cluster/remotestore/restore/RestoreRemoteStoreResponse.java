/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.restore;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.Nullable;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.snapshots.RestoreInfo;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Contains information about remote store restores
 *
 * @opensearch.internal
 */
public final class RestoreRemoteStoreResponse extends ActionResponse implements ToXContentObject {

    @Nullable
    private final RestoreInfo restoreInfo;

    public RestoreRemoteStoreResponse(@Nullable RestoreInfo restoreInfo) {
        this.restoreInfo = restoreInfo;
    }

    public RestoreRemoteStoreResponse(StreamInput in) throws IOException {
        super(in);
        restoreInfo = RestoreInfo.readOptionalRestoreInfo(in);
    }

    /**
     * Returns restore information if remote store restore was completed before this method returned, null otherwise
     *
     * @return restore information or null
     */
    public RestoreInfo getRestoreInfo() {
        return restoreInfo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(restoreInfo);
    }

    public RestStatus status() {
        if (restoreInfo == null) {
            return RestStatus.ACCEPTED;
        }
        return restoreInfo.status();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (restoreInfo != null) {
            builder.field("remote_store");
            restoreInfo.toXContent(builder, params);
        } else {
            builder.field("accepted", true);
        }
        builder.endObject();
        return builder;
    }

    public static final ConstructingObjectParser<RestoreRemoteStoreResponse, Void> PARSER = new ConstructingObjectParser<>(
        "restore_remote_store",
        true,
        v -> {
            RestoreInfo restoreInfo = (RestoreInfo) v[0];
            Boolean accepted = (Boolean) v[1];
            assert (accepted == null && restoreInfo != null) || (accepted != null && accepted && restoreInfo == null) : "accepted: ["
                + accepted
                + "], restoreInfo: ["
                + restoreInfo
                + "]";
            return new RestoreRemoteStoreResponse(restoreInfo);
        }
    );

    static {
        PARSER.declareObject(
            optionalConstructorArg(),
            (parser, context) -> RestoreInfo.fromXContent(parser),
            new ParseField("remote_store")
        );
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField("accepted"));
    }

    public static RestoreRemoteStoreResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestoreRemoteStoreResponse that = (RestoreRemoteStoreResponse) o;
        return Objects.equals(restoreInfo, that.restoreInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(restoreInfo);
    }

    @Override
    public String toString() {
        return "RestoreRemoteStoreResponse{" + "restoreInfo=" + restoreInfo + '}';
    }
}
