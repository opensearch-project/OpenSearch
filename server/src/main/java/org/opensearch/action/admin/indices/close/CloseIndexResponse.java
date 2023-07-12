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

package org.opensearch.action.admin.indices.close;

import org.opensearch.OpenSearchException;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.master.ShardsAcknowledgedResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.util.CollectionUtils;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.index.Index;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;

/**
 * Transport response for closing an index
 *
 * @opensearch.internal
 */
public class CloseIndexResponse extends ShardsAcknowledgedResponse {

    private final List<IndexResult> indices;

    CloseIndexResponse(StreamInput in) throws IOException {
        super(in, true);
        indices = unmodifiableList(in.readList(IndexResult::new));
    }

    public CloseIndexResponse(final boolean acknowledged, final boolean shardsAcknowledged, final List<IndexResult> indices) {
        super(acknowledged, shardsAcknowledged);
        this.indices = unmodifiableList(Objects.requireNonNull(indices));
    }

    public List<IndexResult> getIndices() {
        return indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeShardsAcknowledged(out);
        out.writeList(indices);
    }

    @Override
    protected void addCustomFields(final XContentBuilder builder, final Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.startObject("indices");
        for (IndexResult index : indices) {
            index.toXContent(builder, params);
        }
        builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    /**
     * Inner index result
     *
     * @opensearch.internal
     */
    public static class IndexResult implements Writeable, ToXContentFragment {

        private final Index index;
        private final @Nullable Exception exception;
        private final @Nullable ShardResult[] shards;

        public IndexResult(final Index index) {
            this(index, null, null);
        }

        public IndexResult(final Index index, final Exception failure) {
            this(index, Objects.requireNonNull(failure), null);
        }

        public IndexResult(final Index index, final ShardResult[] shards) {
            this(index, null, Objects.requireNonNull(shards));
        }

        private IndexResult(final Index index, @Nullable final Exception exception, @Nullable final ShardResult[] shards) {
            this.index = Objects.requireNonNull(index);
            this.exception = exception;
            this.shards = shards;
        }

        IndexResult(final StreamInput in) throws IOException {
            this.index = new Index(in);
            this.exception = in.readException();
            this.shards = in.readOptionalArray(ShardResult::new, ShardResult[]::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            index.writeTo(out);
            out.writeException(exception);
            out.writeOptionalArray(shards);
        }

        public Index getIndex() {
            return index;
        }

        public Exception getException() {
            return exception;
        }

        public ShardResult[] getShards() {
            return shards;
        }

        public boolean hasFailures() {
            if (exception != null) {
                return true;
            }
            if (shards != null) {
                for (ShardResult shard : shards) {
                    if (shard.hasFailures()) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject(index.getName());
            {
                if (hasFailures()) {
                    builder.field("closed", false);
                    if (exception != null) {
                        builder.startObject("exception");
                        OpenSearchException.generateFailureXContent(builder, params, exception, true);
                        builder.endObject();
                    } else {
                        builder.startObject("failedShards");
                        for (ShardResult shard : shards) {
                            if (shard.hasFailures()) {
                                shard.toXContent(builder, params);
                            }
                        }
                        builder.endObject();
                    }
                } else {
                    builder.field("closed", true);
                }
            }
            return builder.endObject();
        }

        @Override
        public String toString() {
            return Strings.toString(XContentType.JSON, this);
        }
    }

    /**
     * Shard Result from Close Index Response
     *
     * @opensearch.internal
     */
    public static class ShardResult implements Writeable, ToXContentFragment {

        private final int id;
        private final Failure[] failures;

        public ShardResult(final int id, final Failure[] failures) {
            this.id = id;
            this.failures = failures;
        }

        ShardResult(final StreamInput in) throws IOException {
            this.id = in.readVInt();
            this.failures = in.readOptionalArray(Failure::readFailure, Failure[]::new);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            out.writeVInt(id);
            out.writeOptionalArray(failures);
        }

        public boolean hasFailures() {
            return CollectionUtils.isEmpty(failures) == false;
        }

        public int getId() {
            return id;
        }

        public Failure[] getFailures() {
            return failures;
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject(String.valueOf(id));
            {
                builder.startArray("failures");
                if (failures != null) {
                    for (Failure failure : failures) {
                        failure.toXContent(builder, params);
                    }
                }
                builder.endArray();
            }
            return builder.endObject();
        }

        @Override
        public String toString() {
            return Strings.toString(XContentType.JSON, this);
        }

        /**
         * Inner Failure if something goes wrong
         *
         * @opensearch.internal
         */
        public static class Failure extends DefaultShardOperationFailedException {

            private @Nullable String nodeId;

            private Failure(StreamInput in) throws IOException {
                super(in);
                nodeId = in.readOptionalString();
            }

            public Failure(final String index, final int shardId, final Throwable reason) {
                this(index, shardId, reason, null);
            }

            public Failure(final String index, final int shardId, final Throwable reason, final String nodeId) {
                super(index, shardId, reason);
                this.nodeId = nodeId;
            }

            public String getNodeId() {
                return nodeId;
            }

            @Override
            public void writeTo(final StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeOptionalString(nodeId);
            }

            @Override
            public XContentBuilder innerToXContent(final XContentBuilder builder, final Params params) throws IOException {
                if (nodeId != null) {
                    builder.field("node", nodeId);
                }
                return super.innerToXContent(builder, params);
            }

            @Override
            public String toString() {
                return Strings.toString(XContentType.JSON, this);
            }

            static Failure readFailure(final StreamInput in) throws IOException {
                return new Failure(in);
            }
        }
    }
}
