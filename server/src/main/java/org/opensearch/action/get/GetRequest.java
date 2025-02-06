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

package org.opensearch.action.get;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.RealtimeRequest;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.Requests;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * A request to get a document (its source) from an index based on its id. Best created using
 * {@link Requests#getRequest(String)}.
 * <p>
 * The operation requires the {@link #index()}} and {@link #id(String)}
 * to be set.
 *
 * @see GetResponse
 * @see Requests#getRequest(String)
 * @see Client#get(GetRequest)
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GetRequest extends SingleShardRequest<GetRequest> implements RealtimeRequest {

    private String id;
    private String routing;
    private String preference;

    private String[] storedFields;

    private FetchSourceContext fetchSourceContext;

    private boolean refresh = false;

    boolean realtime = true;

    private VersionType versionType = VersionType.INTERNAL;
    private long version = Versions.MATCH_ANY;

    public GetRequest() {}

    GetRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_2_0_0)) {
            in.readString();
        }
        id = in.readString();
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        refresh = in.readBoolean();
        storedFields = in.readOptionalStringArray();
        realtime = in.readBoolean();

        this.versionType = VersionType.fromValue(in.readByte());
        this.version = in.readLong();
        fetchSourceContext = in.readOptionalWriteable(FetchSourceContext::new);
    }

    /**
     * Constructs a new get request against the specified index. The {@link #id(String)} must also be set.
     */
    public GetRequest(String index) {
        super(index);
    }

    /**
     * Constructs a new get request against the specified index and document ID.
     *
     * @param index The index to get the document from
     * @param id    The id of the document
     */
    public GetRequest(String index, String id) {
        super(index);
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validateNonNullIndex();
        if (Strings.isEmpty(id)) {
            validationException = addValidationError("id is missing", validationException);
        }
        if (versionType.validateVersionForReads(version) == false) {
            validationException = ValidateActions.addValidationError(
                "illegal version value [" + version + "] for version type [" + versionType.name() + "]",
                validationException
            );
        }
        return validationException;
    }

    /**
     * Sets the id of the document to fetch.
     */
    public GetRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public GetRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards, {@code _primary} to execute only on primary shards,
     * or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public GetRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String id() {
        return id;
    }

    public String routing() {
        return this.routing;
    }

    public String preference() {
        return this.preference;
    }

    /**
     * Allows setting the {@link FetchSourceContext} for this request, controlling if and how _source should be returned.
     */
    public GetRequest fetchSourceContext(FetchSourceContext context) {
        this.fetchSourceContext = context;
        return this;
    }

    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }

    /**
     * Explicitly specify the stored fields that will be returned. By default, the {@code _source}
     * field will be returned.
     */
    public GetRequest storedFields(String... fields) {
        this.storedFields = fields;
        return this;
    }

    /**
     * Explicitly specify the stored fields that will be returned. By default, the {@code _source}
     * field will be returned.
     */
    public String[] storedFields() {
        return this.storedFields;
    }

    /**
     * Should a refresh be executed before this get operation causing the operation to
     * return the latest value. Note, heavy get should not set this to {@code true}. Defaults
     * to {@code false}.
     */
    public GetRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public boolean realtime() {
        return this.realtime;
    }

    @Override
    public GetRequest realtime(boolean realtime) {
        this.realtime = realtime;
        return this;
    }

    /**
     * Sets the version, which will cause the get operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public long version() {
        return version;
    }

    public GetRequest version(long version) {
        this.version = version;
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link org.opensearch.index.VersionType#INTERNAL}.
     */
    public GetRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeString(MapperService.SINGLE_MAPPING_NAME);
        }
        out.writeString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);

        out.writeBoolean(refresh);
        out.writeOptionalStringArray(storedFields);
        out.writeBoolean(realtime);
        out.writeByte(versionType.getValue());
        out.writeLong(version);
        out.writeOptionalWriteable(fetchSourceContext);
    }

    @Override
    public String toString() {
        return "get [" + index + "][" + id + "]: routing [" + routing + "]";
    }

}
