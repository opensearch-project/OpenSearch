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

package org.opensearch.client.core;

import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.ParseField;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A response to _count API request.
 */
public final class CountResponse {

    static final ParseField COUNT = new ParseField("count");
    static final ParseField TERMINATED_EARLY = new ParseField("terminated_early");
    static final ParseField SHARDS = new ParseField("_shards");

    private final long count;
    private final Boolean terminatedEarly;
    private final ShardStats shardStats;

    public CountResponse(long count, Boolean terminatedEarly, ShardStats shardStats) {
        this.count = count;
        this.terminatedEarly = terminatedEarly;
        this.shardStats = shardStats;
    }

    public ShardStats getShardStats() {
        return shardStats;
    }

    /**
     * Number of documents matching request.
     */
    public long getCount() {
        return count;
    }

    /**
     * The total number of shards the search was executed on.
     */
    public int getTotalShards() {
        return shardStats.totalShards;
    }

    /**
     * The successful number of shards the search was executed on.
     */
    public int getSuccessfulShards() {
        return shardStats.successfulShards;
    }

    /**
     * The number of shards skipped due to pre-filtering
     */
    public int getSkippedShards() {
        return shardStats.skippedShards;
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int getFailedShards() {
        return shardStats.shardFailures.length;
    }

    /**
     * The failures that occurred during the search.
     */
    public ShardSearchFailure[] getShardFailures() {
        return shardStats.shardFailures;
    }

    public RestStatus status() {
        return RestStatus.status(shardStats.successfulShards, shardStats.totalShards, shardStats.shardFailures);
    }

    public static CountResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String currentName = parser.currentName();
        Boolean terminatedEarly = null;
        long count = 0;
        ShardStats shardStats = new ShardStats(-1, -1, 0, ShardSearchFailure.EMPTY_ARRAY);

        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token.isValue()) {
                if (COUNT.match(currentName, parser.getDeprecationHandler())) {
                    count = parser.longValue();
                } else if (TERMINATED_EARLY.match(currentName, parser.getDeprecationHandler())) {
                    terminatedEarly = parser.booleanValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SHARDS.match(currentName, parser.getDeprecationHandler())) {
                    shardStats = ShardStats.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            }
        }
        return new CountResponse(count, terminatedEarly, shardStats);
    }

    @Override
    public String toString() {
        String s = "{"
            + "count="
            + count
            + (isTerminatedEarly() != null ? ", terminatedEarly=" + terminatedEarly : "")
            + ", "
            + shardStats
            + '}';
        return s;
    }

    public Boolean isTerminatedEarly() {
        return terminatedEarly;
    }

    /**
     * Encapsulates _shards section of count api response.
     */
    public static final class ShardStats {

        static final ParseField FAILED = new ParseField("failed");
        static final ParseField SKIPPED = new ParseField("skipped");
        static final ParseField TOTAL = new ParseField("total");
        static final ParseField SUCCESSFUL = new ParseField("successful");
        static final ParseField FAILURES = new ParseField("failures");

        private final int successfulShards;
        private final int totalShards;
        private final int skippedShards;
        private final ShardSearchFailure[] shardFailures;

        public ShardStats(int successfulShards, int totalShards, int skippedShards, ShardSearchFailure[] shardFailures) {
            this.successfulShards = successfulShards;
            this.totalShards = totalShards;
            this.skippedShards = skippedShards;
            this.shardFailures = Arrays.copyOf(shardFailures, shardFailures.length);
        }

        public int getSuccessfulShards() {
            return successfulShards;
        }

        public int getTotalShards() {
            return totalShards;
        }

        public int getSkippedShards() {
            return skippedShards;
        }

        public ShardSearchFailure[] getShardFailures() {
            return Arrays.copyOf(shardFailures, shardFailures.length, ShardSearchFailure[].class);
        }

        static ShardStats fromXContent(XContentParser parser) throws IOException {
            int successfulShards = -1;
            int totalShards = -1;
            int skippedShards = 0; // BWC @see org.opensearch.action.search.SearchResponse
            List<ShardSearchFailure> failures = new ArrayList<>();
            XContentParser.Token token;
            String currentName = parser.currentName();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentName = parser.currentName();
                } else if (token.isValue()) {
                    if (FAILED.match(currentName, parser.getDeprecationHandler())) {
                        parser.intValue();
                    } else if (SKIPPED.match(currentName, parser.getDeprecationHandler())) {
                        skippedShards = parser.intValue();
                    } else if (TOTAL.match(currentName, parser.getDeprecationHandler())) {
                        totalShards = parser.intValue();
                    } else if (SUCCESSFUL.match(currentName, parser.getDeprecationHandler())) {
                        successfulShards = parser.intValue();
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (FAILURES.match(currentName, parser.getDeprecationHandler())) {
                        while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            failures.add(ShardSearchFailure.fromXContent(parser));
                        }
                    } else {
                        parser.skipChildren();
                    }
                } else {
                    parser.skipChildren();
                }
            }
            return new ShardStats(successfulShards, totalShards, skippedShards, failures.toArray(new ShardSearchFailure[0]));
        }

        @Override
        public String toString() {
            return "_shards : {"
                + "total="
                + totalShards
                + ", successful="
                + successfulShards
                + ", skipped="
                + skippedShards
                + ", failed="
                + (shardFailures != null && shardFailures.length > 0 ? shardFailures.length : 0)
                + (shardFailures != null && shardFailures.length > 0 ? ", failures: " + Arrays.asList(shardFailures) : "")
                + '}';
        }
    }
}
