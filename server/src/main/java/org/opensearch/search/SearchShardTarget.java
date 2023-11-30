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

package org.opensearch.search;

import org.opensearch.action.OriginalIndices;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.Objects;

/**
 * The target that the search request was executed on.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class SearchShardTarget implements Writeable, Comparable<SearchShardTarget> {

    private final Text nodeId;
    private final ShardId shardId;
    // original indices are only needed in the coordinating node throughout the search request execution.
    // no need to serialize them as part of SearchShardTarget.
    private final transient OriginalIndices originalIndices;
    private final String clusterAlias;

    public SearchShardTarget(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            nodeId = in.readText();
        } else {
            nodeId = null;
        }
        shardId = new ShardId(in);
        this.originalIndices = null;
        clusterAlias = in.readOptionalString();
    }

    public SearchShardTarget(String nodeId, ShardId shardId, @Nullable String clusterAlias, OriginalIndices originalIndices) {
        this.nodeId = nodeId == null ? null : new Text(nodeId);
        this.shardId = shardId;
        this.originalIndices = originalIndices;
        this.clusterAlias = clusterAlias;
    }

    @Nullable
    public String getNodeId() {
        return nodeId != null ? nodeId.string() : null;
    }

    public Text getNodeIdText() {
        return this.nodeId;
    }

    public String getIndex() {
        return shardId.getIndexName();
    }

    public ShardId getShardId() {
        return shardId;
    }

    public OriginalIndices getOriginalIndices() {
        return originalIndices;
    }

    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    /**
     * Returns the fully qualified index name, including the index prefix that indicates which cluster results come from.
     */
    public String getFullyQualifiedIndexName() {
        return RemoteClusterAware.buildRemoteIndexName(clusterAlias, getIndex());
    }

    @Override
    public int compareTo(SearchShardTarget o) {
        int i = shardId.getIndexName().compareTo(o.getIndex());
        if (i == 0) {
            i = shardId.getId() - o.shardId.id();
        }
        return i;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (nodeId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeText(nodeId);
        }
        shardId.writeTo(out);
        out.writeOptionalString(clusterAlias);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchShardTarget that = (SearchShardTarget) o;
        return Objects.equals(nodeId, that.nodeId)
            && Objects.equals(shardId, that.shardId)
            && Objects.equals(clusterAlias, that.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, shardId, clusterAlias);
    }

    @Override
    public String toString() {
        String shardToString = "["
            + RemoteClusterAware.buildRemoteIndexName(clusterAlias, shardId.getIndexName())
            + "]["
            + shardId.getId()
            + "]";
        if (nodeId == null) {
            return "[_na_]" + shardToString;
        }
        return "[" + nodeId + "]" + shardToString;
    }
}
