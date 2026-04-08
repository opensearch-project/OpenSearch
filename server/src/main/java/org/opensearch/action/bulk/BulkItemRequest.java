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

package org.opensearch.action.bulk;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;

/**
 * Transport request for a Single bulk item
 *
 * @opensearch.internal
 */
public record BulkItemRequest(int id, DocWriteRequest<?> request, BulkItemResponse primaryResponse) implements Writeable, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class);

    /**
     * @param shardId the shard id
     */
    BulkItemRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        this(in.readVInt(), DocWriteRequest.readDocumentRequest(shardId, in), readPrimaryResponse(shardId, in));
    }

    private static BulkItemResponse readPrimaryResponse(ShardId shardId, StreamInput in) throws IOException {
        if (in.readBoolean()) {
            if (shardId == null) {
                return new BulkItemResponse(in);
            } else {
                return new BulkItemResponse(shardId, in);
            }
        }
        return null;
    }

    // NOTE: public for testing only
    public BulkItemRequest(int id, DocWriteRequest<?> request) {
        this(id, request, null);
    }

    public String index() {
        assert request.indices().length == 1;
        return request.indices()[0];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        DocWriteRequest.writeDocumentRequest(out, request);
        out.writeOptionalWriteable(primaryResponse);
    }

    public void writeThin(StreamOutput out) throws IOException {
        out.writeVInt(id);
        DocWriteRequest.writeDocumentRequestThin(out, request);
        out.writeOptionalWriteable((o, resp) -> resp.writeThin(o), primaryResponse);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + request.ramBytesUsed();
    }
}
