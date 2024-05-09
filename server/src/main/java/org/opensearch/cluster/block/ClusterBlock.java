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

package org.opensearch.cluster.block;

import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.cluster.metadata.Metadata.CONTEXT_MODE_API;
import static org.opensearch.cluster.metadata.Metadata.CONTEXT_MODE_PARAM;

/**
 * Blocks the cluster for concurrency
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterBlock implements Writeable, ToXContentFragment {

    static final String KEY_UUID = "uuid";
    static final String KEY_DESCRIPTION = "description";
    static final String KEY_RETRYABLE = "retryable";
    static final String KEY_DISABLE_STATE_PERSISTENCE = "disable_state_persistence";
    static final String KEY_LEVELS = "levels";
    static final String KEY_STATUS = "status";
    static final String KEY_ALLOW_RELEASE_RESOURCES = "allow_release_resources";
    private static final Set<String> VALID_FIELDS = Sets.newHashSet(
        KEY_UUID,
        KEY_DESCRIPTION,
        KEY_RETRYABLE,
        KEY_DISABLE_STATE_PERSISTENCE,
        KEY_LEVELS
    );
    private final int id;
    @Nullable
    private final String uuid;
    private final String description;
    private final EnumSet<ClusterBlockLevel> levels;
    private final boolean retryable;
    private final boolean disableStatePersistence;
    private final boolean allowReleaseResources;
    private final RestStatus status;

    public ClusterBlock(StreamInput in) throws IOException {
        id = in.readVInt();
        uuid = in.readOptionalString();
        description = in.readString();
        this.levels = in.readEnumSet(ClusterBlockLevel.class);
        retryable = in.readBoolean();
        disableStatePersistence = in.readBoolean();
        status = RestStatus.readFrom(in);
        allowReleaseResources = in.readBoolean();
    }

    public ClusterBlock(
        int id,
        String description,
        boolean retryable,
        boolean disableStatePersistence,
        boolean allowReleaseResources,
        RestStatus status,
        EnumSet<ClusterBlockLevel> levels
    ) {
        this(id, null, description, retryable, disableStatePersistence, allowReleaseResources, status, levels);
    }

    public ClusterBlock(
        int id,
        String uuid,
        String description,
        boolean retryable,
        boolean disableStatePersistence,
        boolean allowReleaseResources,
        RestStatus status,
        EnumSet<ClusterBlockLevel> levels
    ) {
        this.id = id;
        this.uuid = uuid;
        this.description = description;
        this.retryable = retryable;
        this.disableStatePersistence = disableStatePersistence;
        this.status = status;
        this.levels = levels;
        this.allowReleaseResources = allowReleaseResources;
    }

    public int id() {
        return this.id;
    }

    @Nullable
    public String uuid() {
        return uuid;
    }

    public String description() {
        return this.description;
    }

    public RestStatus status() {
        return this.status;
    }

    public EnumSet<ClusterBlockLevel> levels() {
        return this.levels;
    }

    public boolean contains(ClusterBlockLevel level) {
        for (ClusterBlockLevel testLevel : levels) {
            if (testLevel == level) {
                return true;
            }
        }
        return false;
    }

    /**
     * Should operations get into retry state if this block is present.
     */
    public boolean retryable() {
        return this.retryable;
    }

    /**
     * Should global state persistence be disabled when this block is present. Note,
     * only relevant for global blocks.
     */
    public boolean disableStatePersistence() {
        return this.disableStatePersistence;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Metadata.XContentContext context = Metadata.XContentContext.valueOf(params.param(CONTEXT_MODE_PARAM, CONTEXT_MODE_API));
        builder.startObject(Integer.toString(id));
        if (uuid != null) {
            builder.field(KEY_UUID, uuid);
        }
        builder.field(KEY_DESCRIPTION, description);
        builder.field(KEY_RETRYABLE, retryable);
        if (disableStatePersistence) {
            builder.field(KEY_DISABLE_STATE_PERSISTENCE, disableStatePersistence);
        }
        builder.startArray(KEY_LEVELS);
        for (ClusterBlockLevel level : levels) {
            builder.value(level.name().toLowerCase(Locale.ROOT));
        }
        builder.endArray();
        if (context == Metadata.XContentContext.GATEWAY) {
            builder.field(KEY_STATUS, status);
            builder.field(KEY_ALLOW_RELEASE_RESOURCES, allowReleaseResources);
        }
        builder.endObject();
        return builder;
    }

    public static ClusterBlock fromXContent(XContentParser parser, int id) throws IOException {
        String uuid = null;
        String description = null;
        boolean retryable = false;
        boolean disableStatePersistence = false;
        RestStatus status = null;
        boolean allowReleaseResources = false;
        EnumSet<ClusterBlockLevel> levels = EnumSet.noneOf(ClusterBlockLevel.class);
        String currentFieldName = skipBlockID(parser);
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                switch (Objects.requireNonNull(currentFieldName)) {
                    case KEY_UUID:
                        uuid = parser.text();
                        break;
                    case KEY_DESCRIPTION:
                        description = parser.text();
                        break;
                    case KEY_RETRYABLE:
                        retryable = parser.booleanValue();
                        break;
                    case KEY_DISABLE_STATE_PERSISTENCE:
                        disableStatePersistence = parser.booleanValue();
                        break;
                    case KEY_STATUS:
                        status = RestStatus.valueOf(parser.text());
                        break;
                    case KEY_ALLOW_RELEASE_RESOURCES:
                        allowReleaseResources = parser.booleanValue();
                        break;
                    default:
                        throw new IllegalArgumentException("unknown field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (currentFieldName.equals(KEY_LEVELS)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        levels.add(ClusterBlockLevel.fromString(parser.text(), Locale.ROOT));
                    }
                } else {
                    throw new IllegalArgumentException("unknown field [" + currentFieldName + "]");
                }
            } else {
                throw new IllegalArgumentException("unexpected token [" + token + "]");
            }
        }
        return new ClusterBlock(id, uuid, description, retryable, disableStatePersistence, allowReleaseResources, status, levels);
    }

    private static String skipBlockID(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if (VALID_FIELDS.contains(currentFieldName)) {
                    return currentFieldName;
                } else {
                    // we have hit block id, just move on
                    parser.nextToken();
                }
            }
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        out.writeOptionalString(uuid);
        out.writeString(description);
        out.writeEnumSet(levels);
        out.writeBoolean(retryable);
        out.writeBoolean(disableStatePersistence);
        RestStatus.writeTo(out, status);
        out.writeBoolean(allowReleaseResources);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id).append(",");
        if (uuid != null) {
            sb.append(uuid).append(',');
        }
        sb.append(description).append(", blocks ");
        String delimiter = "";
        for (ClusterBlockLevel level : levels) {
            sb.append(delimiter).append(level.name());
            delimiter = ",";
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClusterBlock that = (ClusterBlock) o;
        return id == that.id && Objects.equals(uuid, that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, uuid);
    }

    public boolean isAllowReleaseResources() {
        return allowReleaseResources;
    }
}
