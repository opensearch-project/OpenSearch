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

package org.opensearch.action.admin.indices.validate.query;

import org.opensearch.action.support.broadcast.BroadcastShardResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Internal validate response of a shard validate request executed directly against a specific shard.
 *
 * @opensearch.internal
 */
class ShardValidateQueryResponse extends BroadcastShardResponse {

    private boolean valid;

    private String explanation;

    private String error;

    ShardValidateQueryResponse(StreamInput in) throws IOException {
        super(in);
        valid = in.readBoolean();
        explanation = in.readOptionalString();
        error = in.readOptionalString();
    }

    ShardValidateQueryResponse(ShardId shardId, boolean valid, String explanation, String error) {
        super(shardId);
        this.valid = valid;
        this.explanation = explanation;
        this.error = error;
    }

    public boolean isValid() {
        return this.valid;
    }

    public String getExplanation() {
        return explanation;
    }

    public String getError() {
        return error;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(valid);
        out.writeOptionalString(explanation);
        out.writeOptionalString(error);
    }
}
