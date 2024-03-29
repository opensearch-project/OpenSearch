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

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a shard snapshot in a repository.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class RepositoryShardId implements Writeable {

    private final IndexId index;

    private final int shard;

    public RepositoryShardId(IndexId index, int shard) {
        assert index != null;
        this.index = index;
        this.shard = shard;
    }

    public RepositoryShardId(StreamInput in) throws IOException {
        this(new IndexId(in), in.readVInt());
    }

    public IndexId index() {
        return index;
    }

    public String indexName() {
        return index.getName();
    }

    public int shardId() {
        return shard;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, shard);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof RepositoryShardId == false) {
            return false;
        }
        final RepositoryShardId that = (RepositoryShardId) obj;
        return that.index.equals(index) && that.shard == shard;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        index.writeTo(out);
        out.writeVInt(shard);
    }

    @Override
    public String toString() {
        return "RepositoryShardId{" + index + "}{" + shard + "}";
    }
}
