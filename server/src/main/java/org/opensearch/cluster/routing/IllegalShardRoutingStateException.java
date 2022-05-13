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

package org.opensearch.cluster.routing;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * This exception defines illegal states of shard routing
 *
 * @opensearch.internal
 */
public class IllegalShardRoutingStateException extends RoutingException {

    private final ShardRouting shard;

    public IllegalShardRoutingStateException(ShardRouting shard, String message) {
        this(shard, message, null);
    }

    public IllegalShardRoutingStateException(ShardRouting shard, String message, Throwable cause) {
        super(shard.shortSummary() + ": " + message, cause);
        this.shard = shard;
    }

    public IllegalShardRoutingStateException(StreamInput in) throws IOException {
        super(in);
        shard = new ShardRouting(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shard.writeTo(out);
    }

    /**
     * Returns the shard instance referenced by this exception
     * @return shard instance referenced by this exception
     */
    public ShardRouting shard() {
        return shard;
    }
}
