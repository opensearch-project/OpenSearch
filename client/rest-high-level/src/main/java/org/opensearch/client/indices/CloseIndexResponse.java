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

package org.opensearch.client.indices;

import org.opensearch.OpenSearchException;
import org.opensearch.action.support.clustermanager.ShardsAcknowledgedResponse;
import org.opensearch.common.Nullable;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.opensearch.core.xcontent.ObjectParser.ValueType;

public class CloseIndexResponse extends ShardsAcknowledgedResponse {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<CloseIndexResponse, Void> PARSER = new ConstructingObjectParser<>(
        "close_index_response",
        true,
        args -> {
            boolean acknowledged = (boolean) args[0];
            boolean shardsAcknowledged = args[1] != null ? (boolean) args[1] : acknowledged;
            List<CloseIndexResponse.IndexResult> indices = args[2] != null ? (List<CloseIndexResponse.IndexResult>) args[2] : emptyList();
            return new CloseIndexResponse(acknowledged, shardsAcknowledged, indices);
        }
    );

    static {
        declareAcknowledgedField(PARSER);
        PARSER.declareField(optionalConstructorArg(), (parser, context) -> parser.booleanValue(), SHARDS_ACKNOWLEDGED, ValueType.BOOLEAN);
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, name) -> IndexResult.fromXContent(p, name), new ParseField("indices"));
    }

    private final List<CloseIndexResponse.IndexResult> indices;

    public CloseIndexResponse(final boolean acknowledged, final boolean shardsAcknowledged, final List<IndexResult> indices) {
        super(acknowledged, shardsAcknowledged);
        this.indices = unmodifiableList(Objects.requireNonNull(indices));
    }

    public List<IndexResult> getIndices() {
        return indices;
    }

    public static CloseIndexResponse fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static class IndexResult {

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<IndexResult, String> PARSER = new ConstructingObjectParser<>(
            "index_result",
            true,
            (args, index) -> {
                Exception exception = (Exception) args[1];
                if (exception != null) {
                    assert (boolean) args[0] == false;
                    return new IndexResult(index, exception);
                }
                ShardResult[] shardResults = args[2] != null ? ((List<ShardResult>) args[2]).toArray(new ShardResult[0]) : null;
                if (shardResults != null) {
                    assert (boolean) args[0] == false;
                    return new IndexResult(index, shardResults);
                }
                assert (boolean) args[0];
                return new IndexResult(index);
            }
        );
        static {
            PARSER.declareBoolean(optionalConstructorArg(), new ParseField("closed"));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, p.currentToken(), p);
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, p.nextToken(), p);
                Exception e = OpenSearchException.failureFromXContent(p);
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, p.nextToken(), p);
                return e;
            }, new ParseField("exception"));
            PARSER.declareNamedObjects(
                optionalConstructorArg(),
                (p, c, id) -> ShardResult.fromXContent(p, id),
                new ParseField("failedShards")
            );
        }

        private final String index;
        private final @Nullable Exception exception;
        private final @Nullable ShardResult[] shards;

        IndexResult(final String index) {
            this(index, null, null);
        }

        IndexResult(final String index, final Exception failure) {
            this(index, Objects.requireNonNull(failure), null);
        }

        IndexResult(final String index, final ShardResult[] shards) {
            this(index, null, Objects.requireNonNull(shards));
        }

        private IndexResult(final String index, @Nullable final Exception exception, @Nullable final ShardResult[] shards) {
            this.index = Objects.requireNonNull(index);
            this.exception = exception;
            this.shards = shards;
        }

        public String getIndex() {
            return index;
        }

        public @Nullable Exception getException() {
            return exception;
        }

        public @Nullable ShardResult[] getShards() {
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

        static IndexResult fromXContent(final XContentParser parser, final String name) {
            return PARSER.apply(parser, name);
        }
    }

    public static class ShardResult {

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ShardResult, String> PARSER = new ConstructingObjectParser<>(
            "shard_result",
            true,
            (arg, id) -> {
                Failure[] failures = arg[0] != null ? ((List<Failure>) arg[0]).toArray(new Failure[0]) : new Failure[0];
                return new ShardResult(Integer.parseInt(id), failures);
            }
        );

        static {
            PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> Failure.PARSER.apply(p, null), new ParseField("failures"));
        }

        private final int id;
        private final Failure[] failures;

        ShardResult(final int id, final Failure[] failures) {
            this.id = id;
            this.failures = failures;
        }

        public boolean hasFailures() {
            return failures != null && failures.length > 0;
        }

        public int getId() {
            return id;
        }

        public Failure[] getFailures() {
            return failures;
        }

        static ShardResult fromXContent(final XContentParser parser, final String id) {
            return PARSER.apply(parser, id);
        }

        public static class Failure extends DefaultShardOperationFailedException {

            static final ConstructingObjectParser<Failure, Void> PARSER = new ConstructingObjectParser<>(
                "failure",
                true,
                arg -> new Failure((String) arg[0], (int) arg[1], (Throwable) arg[2], (String) arg[3])
            );

            static {
                declareFields(PARSER);
                PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("node"));
            }

            private @Nullable String nodeId;

            Failure(final String index, final int shardId, final Throwable reason, final String nodeId) {
                super(index, shardId, reason);
                this.nodeId = nodeId;
            }

            public String getNodeId() {
                return nodeId;
            }
        }
    }
}
