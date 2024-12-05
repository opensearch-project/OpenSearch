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
import org.opensearch.Version;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.CompositeIndicesRequest;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.replication.ReplicationRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A bulk request holds an ordered {@link IndexRequest}s, {@link DeleteRequest}s and {@link UpdateRequest}s
 * and allows to executes it in a single batch.
 * <p>
 * Note that we only support refresh on the bulk request not per item.
 * @see org.opensearch.client.Client#bulk(BulkRequest)
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class BulkRequest extends ActionRequest implements CompositeIndicesRequest, WriteRequest<BulkRequest>, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BulkRequest.class);

    private static final int REQUEST_OVERHEAD = 50;
    /**
     * Requests that are part of this request. It is only possible to add things that are both {@link ActionRequest}s and
     * {@link WriteRequest}s to this but java doesn't support syntax to declare that everything in the array has both types so we declare
     * the one with the least casts.
     */
    final List<DocWriteRequest<?>> requests = new ArrayList<>();
    private final Set<String> indices = new HashSet<>();

    protected TimeValue timeout = BulkShardRequest.DEFAULT_TIMEOUT;
    private ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;
    private RefreshPolicy refreshPolicy = RefreshPolicy.NONE;
    private String globalPipeline;
    private String globalRouting;
    private String globalIndex;
    private Boolean globalRequireAlias;
    private int batchSize = Integer.MAX_VALUE;

    private long sizeInBytes = 0;

    public BulkRequest() {}

    public BulkRequest(StreamInput in) throws IOException {
        super(in);
        waitForActiveShards = ActiveShardCount.readFrom(in);
        requests.addAll(in.readList(i -> DocWriteRequest.readDocumentRequest(null, i)));
        refreshPolicy = RefreshPolicy.readFrom(in);
        timeout = in.readTimeValue();
        if (in.getVersion().onOrAfter(Version.V_2_14_0)) {
            batchSize = in.readInt();
        }
    }

    public BulkRequest(@Nullable String globalIndex) {
        this.globalIndex = globalIndex;
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public BulkRequest add(DocWriteRequest<?>... requests) {
        for (DocWriteRequest<?> request : requests) {
            add(request);
        }
        return this;
    }

    /**
     * Add a request to the current BulkRequest.
     * <p>
     * Note for internal callers: This method does not respect all global parameters.
     *                            Only the global index is applied to the request objects.
     *                            Global parameters would be respected if the request was serialized for a REST call as it is
     *                            in the high level rest client.
     * @param request Request to add
     * @return the current bulk request
     */
    public BulkRequest add(DocWriteRequest<?> request) {
        if (request instanceof IndexRequest) {
            add((IndexRequest) request);
        } else if (request instanceof DeleteRequest) {
            add((DeleteRequest) request);
        } else if (request instanceof UpdateRequest) {
            add((UpdateRequest) request);
        } else {
            throw new IllegalArgumentException("No support for request [" + request + "]");
        }
        indices.add(request.index());
        return this;
    }

    /**
     * Adds a list of requests to be executed. Either index or delete requests.
     */
    public BulkRequest add(Iterable<DocWriteRequest<?>> requests) {
        for (DocWriteRequest<?> request : requests) {
            add(request);
        }
        return this;
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkRequest add(IndexRequest request) {
        return internalAdd(request);
    }

    BulkRequest internalAdd(IndexRequest request) {
        Objects.requireNonNull(request, "'request' must not be null");
        applyGlobalMandatoryParameters(request);

        requests.add(request);
        // lack of source is validated in validate() method
        sizeInBytes += (request.source() != null ? request.source().length() : 0) + REQUEST_OVERHEAD;
        indices.add(request.index());
        return this;
    }

    /**
     * Adds an {@link UpdateRequest} to the list of actions to execute.
     */
    public BulkRequest add(UpdateRequest request) {
        return internalAdd(request);
    }

    BulkRequest internalAdd(UpdateRequest request) {
        Objects.requireNonNull(request, "'request' must not be null");
        applyGlobalMandatoryParameters(request);

        requests.add(request);
        if (request.doc() != null) {
            sizeInBytes += request.doc().source().length();
        }
        if (request.upsertRequest() != null) {
            sizeInBytes += request.upsertRequest().source().length();
        }
        if (request.script() != null) {
            sizeInBytes += request.script().getIdOrCode().length() * 2;
        }
        indices.add(request.index());
        return this;
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkRequest add(DeleteRequest request) {
        Objects.requireNonNull(request, "'request' must not be null");
        applyGlobalMandatoryParameters(request);

        requests.add(request);
        sizeInBytes += REQUEST_OVERHEAD;
        indices.add(request.index());
        return this;
    }

    /**
     * The list of requests in this bulk request.
     */
    public List<DocWriteRequest<?>> requests() {
        return this.requests;
    }

    /**
     * The number of actions in the bulk request.
     */
    public int numberOfActions() {
        return requests.size();
    }

    /**
     * The estimated size in bytes of the bulk request.
     */
    public long estimatedSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(byte[] data, int from, int length, MediaType mediaType) throws IOException {
        return add(data, from, length, null, mediaType);
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(byte[] data, int from, int length, @Nullable String defaultIndex, MediaType mediaType) throws IOException {
        return add(new BytesArray(data, from, length), defaultIndex, mediaType);
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(BytesReference data, @Nullable String defaultIndex, MediaType mediaType) throws IOException {
        return add(data, defaultIndex, null, null, null, null, true, mediaType);
    }

    /**
     * Adds a framed data in binary format
     */
    public BulkRequest add(BytesReference data, @Nullable String defaultIndex, boolean allowExplicitIndex, MediaType mediaType)
        throws IOException {
        return add(data, defaultIndex, null, null, null, null, allowExplicitIndex, mediaType);
    }

    public BulkRequest add(
        BytesReference data,
        @Nullable String defaultIndex,
        @Nullable String defaultRouting,
        @Nullable FetchSourceContext defaultFetchSourceContext,
        @Nullable String defaultPipeline,
        boolean allowExplicitIndex,
        MediaType mediaType
    ) throws IOException {
        return add(data, defaultIndex, defaultRouting, defaultFetchSourceContext, defaultPipeline, null, allowExplicitIndex, mediaType);
    }

    public BulkRequest add(
        BytesReference data,
        @Nullable String defaultIndex,
        @Nullable String defaultRouting,
        @Nullable FetchSourceContext defaultFetchSourceContext,
        @Nullable String defaultPipeline,
        @Nullable Boolean defaultRequireAlias,
        boolean allowExplicitIndex,
        MediaType mediaType
    ) throws IOException {
        String routing = valueOrDefault(defaultRouting, globalRouting);
        String pipeline = valueOrDefault(defaultPipeline, globalPipeline);
        Boolean requireAlias = valueOrDefault(defaultRequireAlias, globalRequireAlias);
        new BulkRequestParser().parse(
            data,
            defaultIndex,
            routing,
            defaultFetchSourceContext,
            pipeline,
            requireAlias,
            allowExplicitIndex,
            mediaType,
            this::internalAdd,
            this::internalAdd,
            this::add
        );
        return this;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public BulkRequest waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public BulkRequest waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    public ActiveShardCount waitForActiveShards() {
        return this.waitForActiveShards;
    }

    @Override
    public BulkRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final BulkRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Set batch size
     * @param size batch size from input
     * @return {@link BulkRequest}
     */
    public BulkRequest batchSize(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("batch_size must be greater than 0");
        }
        this.batchSize = size;
        return this;
    }

    /**
     * Get batch size
     * @return batch size
     */
    public int batchSize() {
        return this.batchSize;
    }

    /**
     * Note for internal callers (NOT high level rest client),
     * the global parameter setting is ignored when used with:
     * <p>
     * - {@link BulkRequest#add(IndexRequest)}
     * - {@link BulkRequest#add(UpdateRequest)}
     * - {@link BulkRequest#add(DocWriteRequest)}
     * - {@link BulkRequest#add(DocWriteRequest[])} )}
     * - {@link BulkRequest#add(Iterable)}
     * @param globalPipeline the global default setting
     * @return Bulk request with global setting set
     */
    public final BulkRequest pipeline(String globalPipeline) {
        this.globalPipeline = globalPipeline;
        return this;
    }

    /**
     * Note for internal callers (NOT high level rest client),
     * the global parameter setting is ignored when used with:
     * <p>
      - {@link BulkRequest#add(IndexRequest)}
      - {@link BulkRequest#add(UpdateRequest)}
      - {@link BulkRequest#add(DocWriteRequest)}
      - {@link BulkRequest#add(DocWriteRequest[])} )}
      - {@link BulkRequest#add(Iterable)}
     * @param globalRouting the global default setting
     * @return Bulk request with global setting set
     */
    public final BulkRequest routing(String globalRouting) {
        this.globalRouting = globalRouting;
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final BulkRequest timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".timeout"));
    }

    public TimeValue timeout() {
        return timeout;
    }

    public String pipeline() {
        return globalPipeline;
    }

    public String routing() {
        return globalRouting;
    }

    public Boolean requireAlias() {
        return globalRequireAlias;
    }

    /**
     * Note for internal callers (NOT high level rest client),
     * the global parameter setting is ignored when used with:
     * <p>
     * - {@link BulkRequest#add(IndexRequest)}
     * - {@link BulkRequest#add(UpdateRequest)}
     * - {@link BulkRequest#add(DocWriteRequest)}
     * - {@link BulkRequest#add(DocWriteRequest[])} )}
     * - {@link BulkRequest#add(Iterable)}
     * @param globalRequireAlias the global default setting
     * @return Bulk request with global setting set
     */
    public BulkRequest requireAlias(Boolean globalRequireAlias) {
        this.globalRequireAlias = globalRequireAlias;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (requests.isEmpty()) {
            validationException = addValidationError("no requests added", validationException);
        }
        for (DocWriteRequest<?> request : requests) {
            // We first check if refresh has been set
            if (((WriteRequest<?>) request).getRefreshPolicy() != RefreshPolicy.NONE) {
                validationException = addValidationError(
                    "RefreshPolicy is not supported on an item request. Set it on the BulkRequest instead.",
                    validationException
                );
            }
            ActionRequestValidationException ex = ((WriteRequest<?>) request).validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
        }

        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        waitForActiveShards.writeTo(out);
        out.writeCollection(requests, DocWriteRequest::writeDocumentRequest);
        refreshPolicy.writeTo(out);
        out.writeTimeValue(timeout);
        if (out.getVersion().onOrAfter(Version.V_2_14_0)) {
            out.writeInt(batchSize);
        }
    }

    @Override
    public String getDescription() {
        return "requests[" + requests.size() + "], indices[" + Strings.collectionToDelimitedString(indices, ", ") + "]";
    }

    private void applyGlobalMandatoryParameters(DocWriteRequest<?> request) {
        request.index(valueOrDefault(request.index(), globalIndex));
    }

    private static String valueOrDefault(String value, String globalDefault) {
        if (Strings.isNullOrEmpty(value) && Strings.isNullOrEmpty(globalDefault) == false) {
            return globalDefault;
        }
        return value;
    }

    private static Boolean valueOrDefault(Boolean value, Boolean globalDefault) {
        if (Objects.isNull(value) && !Objects.isNull(globalDefault)) {
            return globalDefault;
        }
        return value;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + requests.stream().mapToLong(Accountable::ramBytesUsed).sum();
    }

    public Set<String> getIndices() {
        return Collections.unmodifiableSet(indices);
    }
}
