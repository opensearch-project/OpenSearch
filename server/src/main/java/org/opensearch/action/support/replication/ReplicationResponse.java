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

package org.opensearch.action.support.replication;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.action.ShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Base class for write action responses.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ReplicationResponse extends ActionResponse {

    public static final ReplicationResponse.ShardInfo.Failure[] EMPTY = new ReplicationResponse.ShardInfo.Failure[0];

    private ShardInfo shardInfo;

    public ReplicationResponse() {}

    public ReplicationResponse(StreamInput in) throws IOException {
        super(in);
        shardInfo = new ReplicationResponse.ShardInfo(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardInfo.writeTo(out);
    }

    public ShardInfo getShardInfo() {
        return shardInfo;
    }

    public void setShardInfo(ShardInfo shardInfo) {
        this.shardInfo = shardInfo;
    }

    /**
     * Holds shard information
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class ShardInfo implements Writeable, ToXContentObject {

        private static final String TOTAL = "total";
        private static final String SUCCESSFUL = "successful";
        private static final String FAILED = "failed";
        private static final String FAILURES = "failures";

        private int total;
        private int successful;
        private Failure[] failures = EMPTY;

        public ShardInfo() {}

        public ShardInfo(StreamInput in) throws IOException {
            total = in.readVInt();
            successful = in.readVInt();
            int size = in.readVInt();
            if (size > 0) {
                failures = new Failure[size];
                for (int i = 0; i < size; i++) {
                    failures[i] = new Failure(in);
                }
            }
        }

        public ShardInfo(int total, int successful, Failure... failures) {
            assert total >= 0 && successful >= 0;
            this.total = total;
            this.successful = successful;
            this.failures = failures;
        }

        /**
         * @return the total number of shards the write should go to (replicas and primaries). This includes relocating shards, so this
         *         number can be higher than the number of shards.
         */
        public int getTotal() {
            return total;
        }

        /**
         * @return the total number of shards the write succeeded on (replicas and primaries). This includes relocating shards, so this
         *         number can be higher than the number of shards.
         */
        public int getSuccessful() {
            return successful;
        }

        /**
         * @return The total number of replication failures.
         */
        public int getFailed() {
            return failures.length;
        }

        /**
         * @return The replication failures that have been captured in the case writes have failed on replica shards.
         */
        public Failure[] getFailures() {
            return failures;
        }

        public RestStatus status() {
            RestStatus status = RestStatus.OK;
            for (Failure failure : failures) {
                if (failure.primary() && failure.status().getStatus() > status.getStatus()) {
                    status = failure.status();
                }
            }
            return status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeVInt(successful);
            out.writeVInt(failures.length);
            for (Failure failure : failures) {
                failure.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TOTAL, total);
            builder.field(SUCCESSFUL, successful);
            builder.field(FAILED, getFailed());
            if (failures.length > 0) {
                builder.startArray(FAILURES);
                for (Failure failure : failures) {
                    failure.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

        public static ShardInfo fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

            int total = 0, successful = 0;
            List<Failure> failuresList = null;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (TOTAL.equals(currentFieldName)) {
                        total = parser.intValue();
                    } else if (SUCCESSFUL.equals(currentFieldName)) {
                        successful = parser.intValue();
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (FAILURES.equals(currentFieldName)) {
                        failuresList = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            failuresList.add(Failure.fromXContent(parser));
                        }
                    } else {
                        parser.skipChildren(); // skip potential inner arrays for forward compatibility
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    parser.skipChildren(); // skip potential inner arrays for forward compatibility
                }
            }
            Failure[] failures = EMPTY;
            if (failuresList != null) {
                failures = failuresList.toArray(new Failure[0]);
            }
            return new ShardInfo(total, successful, failures);
        }

        @Override
        public String toString() {
            return "ShardInfo{" + "total=" + total + ", successful=" + successful + ", failures=" + Arrays.toString(failures) + '}';
        }

        /**
         * Holds failure information
         *
         * @opensearch.api
         */
        @PublicApi(since = "1.0.0")
        public static class Failure extends ShardOperationFailedException implements ToXContentObject {

            private static final String _INDEX = "_index";
            private static final String _SHARD = "_shard";
            private static final String _NODE = "_node";
            private static final String REASON = "reason";
            private static final String STATUS = "status";
            private static final String PRIMARY = "primary";

            private final ShardId shardId;
            private final String nodeId;
            private final boolean primary;

            public Failure(StreamInput in) throws IOException {
                shardId = new ShardId(in);
                super.shardId = shardId.getId();
                index = shardId.getIndexName();
                nodeId = in.readOptionalString();
                cause = in.readException();
                status = RestStatus.readFrom(in);
                primary = in.readBoolean();
            }

            public Failure(ShardId shardId, @Nullable String nodeId, Exception cause, RestStatus status, boolean primary) {
                super(shardId.getIndexName(), shardId.getId(), ExceptionsHelper.detailedMessage(cause), status, cause);
                this.shardId = shardId;
                this.nodeId = nodeId;
                this.primary = primary;
            }

            public ShardId fullShardId() {
                return shardId;
            }

            /**
             * @return On what node the failure occurred.
             */
            @Nullable
            public String nodeId() {
                return nodeId;
            }

            /**
             * @return Whether this failure occurred on a primary shard.
             * (this only reports true for delete by query)
             */
            public boolean primary() {
                return primary;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                shardId.writeTo(out);
                out.writeOptionalString(nodeId);
                out.writeException(cause);
                RestStatus.writeTo(out, status);
                out.writeBoolean(primary);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(_INDEX, shardId.getIndexName());
                builder.field(_SHARD, shardId.id());
                builder.field(_NODE, nodeId);
                builder.field(REASON);
                builder.startObject();
                OpenSearchException.generateThrowableXContent(builder, params, cause);
                builder.endObject();
                builder.field(STATUS, status);
                builder.field(PRIMARY, primary);
                builder.endObject();
                return builder;
            }

            public static Failure fromXContent(XContentParser parser) throws IOException {
                XContentParser.Token token = parser.currentToken();
                ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

                String shardIndex = null, nodeId = null;
                int shardId = -1;
                boolean primary = false;
                RestStatus status = null;
                OpenSearchException reason = null;

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (_INDEX.equals(currentFieldName)) {
                            shardIndex = parser.text();
                        } else if (_SHARD.equals(currentFieldName)) {
                            shardId = parser.intValue();
                        } else if (_NODE.equals(currentFieldName)) {
                            nodeId = parser.text();
                        } else if (STATUS.equals(currentFieldName)) {
                            status = RestStatus.valueOf(parser.text());
                        } else if (PRIMARY.equals(currentFieldName)) {
                            primary = parser.booleanValue();
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (REASON.equals(currentFieldName)) {
                            reason = OpenSearchException.fromXContent(parser);
                        } else {
                            parser.skipChildren(); // skip potential inner objects for forward compatibility
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        parser.skipChildren(); // skip potential inner arrays for forward compatibility
                    }
                }
                return new Failure(new ShardId(shardIndex, IndexMetadata.INDEX_UUID_NA_VALUE, shardId), nodeId, reason, status, primary);
            }
        }
    }
}
