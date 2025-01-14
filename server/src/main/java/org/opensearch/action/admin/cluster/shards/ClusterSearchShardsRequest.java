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

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.slice.SliceBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Transport request for searching shards
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterSearchShardsRequest extends ClusterManagerNodeReadRequest<ClusterSearchShardsRequest>
    implements
        IndicesRequest.Replaceable {

    private String[] indices = Strings.EMPTY_ARRAY;
    @Nullable
    private String routing;
    @Nullable
    private String preference;
    private IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();
    @Nullable
    private SliceBuilder sliceBuilder;

    public ClusterSearchShardsRequest() {}

    public ClusterSearchShardsRequest(String... indices) {
        indices(indices);
    }

    public ClusterSearchShardsRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();

        routing = in.readOptionalString();
        preference = in.readOptionalString();

        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            boolean hasSlice = in.readBoolean();
            if (hasSlice) {
                sliceBuilder = new SliceBuilder(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeOptionalString(routing);
        out.writeOptionalString(preference);
        indicesOptions.writeIndicesOptions(out);
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            if (sliceBuilder != null) {
                out.writeBoolean(true);
                sliceBuilder.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Sets the indices the search will be executed on.
     */
    @Override
    public ClusterSearchShardsRequest indices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (int i = 0; i < indices.length; i++) {
            Objects.requireNonNull(indices[i], "indices[" + i + "] must not be null");
        }
        this.indices = indices;
        return this;
    }

    /**
     * The indices
     */
    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public ClusterSearchShardsRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public ClusterSearchShardsRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public ClusterSearchShardsRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards, {@code _primary} to execute only on primary shards,
     * or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public ClusterSearchShardsRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    public ClusterSearchShardsRequest slice(SliceBuilder sliceBuilder) {
        this.sliceBuilder = sliceBuilder;
        return this;
    }

    public SliceBuilder slice() {
        return this.sliceBuilder;
    }
}
