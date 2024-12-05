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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.repositories;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteStoreEnums;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a single snapshotted index in the repository.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class IndexId implements Writeable, ToXContentObject {
    static final String NAME = "name";
    static final String ID = "id";
    static final String SHARD_PATH_TYPE = "shard_path_type";
    public static final int DEFAULT_SHARD_PATH_TYPE = RemoteStoreEnums.PathType.FIXED.getCode();

    private final String name;
    private final String id;
    private final int shardPathType;
    private final int hashCode;

    // Used for testing only
    public IndexId(final String name, final String id) {
        this(name, id, DEFAULT_SHARD_PATH_TYPE);
    }

    public IndexId(String name, String id, int shardPathType) {
        this.name = name;
        this.id = id;
        this.shardPathType = shardPathType;
        this.hashCode = computeHashCode();
    }

    public IndexId(final StreamInput in) throws IOException {
        this.name = in.readString();
        this.id = in.readString();
        if (in.getVersion().onOrAfter(Version.V_2_17_0)) {
            this.shardPathType = in.readVInt();
        } else {
            this.shardPathType = DEFAULT_SHARD_PATH_TYPE;
        }
        this.hashCode = computeHashCode();
    }

    /**
     * The name of the index.
     */
    public String getName() {
        return name;
    }

    /**
     * The unique ID for the index within the repository.  This is *not* the same as the
     * index's UUID, but merely a unique file/URL friendly identifier that a repository can
     * use to name blobs for the index.
     * <p>
     * We could not use the index's actual UUID (See {@link Index#getUUID()}) because in the
     * case of snapshot/restore, the index UUID in the snapshotted index will be different
     * from the index UUID assigned to it when it is restored. Hence, the actual index UUID
     * is not useful in the context of snapshot/restore for tying a snapshotted index to the
     * index it was snapshot from, and so we are using a separate UUID here.
     */
    public String getId() {
        return id;
    }

    /**
     * The storage path type in remote store for the indexes having the underlying index ids.
     */
    public int getShardPathType() {
        return shardPathType;
    }

    @Override
    public String toString() {
        return "[" + name + "/" + id + "/" + shardPathType + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexId that = (IndexId) o;
        return Objects.equals(name, that.name) && Objects.equals(id, that.id) && Objects.equals(this.shardPathType, that.shardPathType);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int computeHashCode() {
        return Objects.hash(name, id, shardPathType);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(id);
        if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
            out.writeVInt(shardPathType);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(NAME, name);
        builder.field(ID, id);
        builder.field(SHARD_PATH_TYPE, shardPathType);
        builder.endObject();
        return builder;
    }
}
