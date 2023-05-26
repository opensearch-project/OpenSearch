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
import org.opensearch.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

/**
 * Transport request for a Single bulk item
 *
 * @opensearch.internal
 */
public class BulkItemRequest implements Writeable, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BulkItemRequest.class);

    private int id;
    private DocWriteRequest<?> request;
    private volatile BulkItemResponse primaryResponse;

    /**
     * @param shardId the shard id
     */
    BulkItemRequest(@Nullable ShardId shardId, StreamInput in) throws IOException {
        id = in.readVInt();
        request = DocWriteRequest.readDocumentRequest(shardId, in);
        if (in.readBoolean()) {
            if (shardId == null) {
                primaryResponse = new BulkItemResponse(in);
            } else {
                primaryResponse = new BulkItemResponse(shardId, in);
            }
        }
    }

    // NOTE: public for testing only
    public BulkItemRequest(int id, DocWriteRequest<?> request) {
        this.id = id;
        this.request = request;
    }

    public int id() {
        return id;
    }

    public DocWriteRequest<?> request() {
        return request;
    }

    public String index() {
        assert request.indices().length == 1;
        return request.indices()[0];
    }

    BulkItemResponse getPrimaryResponse() {
        return primaryResponse;
    }

    void setPrimaryResponse(BulkItemResponse primaryResponse) {
        this.primaryResponse = primaryResponse;
    }

    /**
     * Abort this request, and store a {@link org.opensearch.action.bulk.BulkItemResponse.Failure} response.
     *
     * @param index The concrete index that was resolved for this request
     * @param cause The cause of the rejection (may not be null)
     * @throws IllegalStateException If a response already exists for this request
     */
    public void abort(String index, Exception cause) {
        if (primaryResponse == null) {
            final BulkItemResponse.Failure failure = new BulkItemResponse.Failure(index, request.id(), Objects.requireNonNull(cause), true);
            setPrimaryResponse(new BulkItemResponse(id, request.opType(), failure));
        } else {
            assert primaryResponse.isFailed() && primaryResponse.getFailure().isAborted() : "response ["
                + Strings.toString(XContentType.JSON, primaryResponse)
                + "]; cause ["
                + cause
                + "]";
            if (primaryResponse.isFailed() && primaryResponse.getFailure().isAborted()) {
                primaryResponse.getFailure().getCause().addSuppressed(cause);
            } else {
                throw new IllegalStateException(
                    "aborting item that with response [" + primaryResponse + "] that was previously processed",
                    cause
                );
            }
        }
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
        out.writeOptionalWriteable(primaryResponse == null ? null : primaryResponse::writeThin);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + request.ramBytesUsed();
    }
}
