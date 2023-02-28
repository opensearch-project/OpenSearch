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

package org.opensearch.action.admin.indices.cache.clear;

import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * The response of a clear cache action.
 *
 * @opensearch.internal
 */
public class ClearIndicesCacheResponse extends BroadcastResponse {

    private static final ConstructingObjectParser<ClearIndicesCacheResponse, Void> PARSER = new ConstructingObjectParser<>(
        "clear_cache",
        true,
        arg -> {
            BroadcastResponse response = (BroadcastResponse) arg[0];
            return new ClearIndicesCacheResponse(
                response.getTotalShards(),
                response.getSuccessfulShards(),
                response.getFailedShards(),
                Arrays.asList(response.getShardFailures())
            );
        }
    );

    static {
        declareBroadcastFields(PARSER);
    }

    ClearIndicesCacheResponse(StreamInput in) throws IOException {
        super(in);
    }

    ClearIndicesCacheResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
    }

    public static ClearIndicesCacheResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
