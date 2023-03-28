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

package org.opensearch.action.admin.cluster.snapshots.create;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.Nullable;
import org.opensearch.core.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotInfo.SnapshotInfoBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Create snapshot response
 *
 * @opensearch.internal
 */
public class CreateSnapshotResponse extends ActionResponse implements ToXContentObject {

    private static final ObjectParser<CreateSnapshotResponse, Void> PARSER = new ObjectParser<>(
        CreateSnapshotResponse.class.getName(),
        true,
        CreateSnapshotResponse::new
    );

    static {
        PARSER.declareObject(
            CreateSnapshotResponse::setSnapshotInfoFromBuilder,
            SnapshotInfo.SNAPSHOT_INFO_PARSER,
            new ParseField("snapshot")
        );
    }

    @Nullable
    private SnapshotInfo snapshotInfo;

    CreateSnapshotResponse() {}

    public CreateSnapshotResponse(@Nullable SnapshotInfo snapshotInfo) {
        this.snapshotInfo = snapshotInfo;
    }

    public CreateSnapshotResponse(StreamInput in) throws IOException {
        super(in);
        snapshotInfo = in.readOptionalWriteable(SnapshotInfo::new);
    }

    private void setSnapshotInfoFromBuilder(SnapshotInfoBuilder snapshotInfoBuilder) {
        this.snapshotInfo = snapshotInfoBuilder.build();
    }

    /**
     * Returns snapshot information if snapshot was completed by the time this method returned or null otherwise.
     *
     * @return snapshot information or null
     */
    public SnapshotInfo getSnapshotInfo() {
        return snapshotInfo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(snapshotInfo);
    }

    /**
     * Returns HTTP status
     * <ul>
     * <li>{@link RestStatus#ACCEPTED} if snapshot is still in progress</li>
     * <li>{@link RestStatus#OK} if snapshot was successful or partially successful</li>
     * <li>{@link RestStatus#INTERNAL_SERVER_ERROR} if snapshot failed completely</li>
     * </ul>
     */
    public RestStatus status() {
        if (snapshotInfo == null) {
            return RestStatus.ACCEPTED;
        }
        return snapshotInfo.status();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (snapshotInfo != null) {
            builder.field("snapshot");
            snapshotInfo.toXContent(builder, params);
        } else {
            builder.field("accepted", true);
        }
        builder.endObject();
        return builder;
    }

    public static CreateSnapshotResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return "CreateSnapshotResponse{" + "snapshotInfo=" + snapshotInfo + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateSnapshotResponse that = (CreateSnapshotResponse) o;
        return Objects.equals(snapshotInfo, that.snapshotInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotInfo);
    }
}
