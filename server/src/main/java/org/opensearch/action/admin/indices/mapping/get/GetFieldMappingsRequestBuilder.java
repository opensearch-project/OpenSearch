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

package org.opensearch.action.admin.indices.mapping.get;

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.ArrayUtils;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * A helper class to build {@link GetFieldMappingsRequest} objects
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GetFieldMappingsRequestBuilder extends ActionRequestBuilder<GetFieldMappingsRequest, GetFieldMappingsResponse> {

    public GetFieldMappingsRequestBuilder(OpenSearchClient client, GetFieldMappingsAction action, String... indices) {
        super(client, action, new GetFieldMappingsRequest().indices(indices));
    }

    public GetFieldMappingsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public GetFieldMappingsRequestBuilder addIndices(String... indices) {
        request.indices(ArrayUtils.concat(request.indices(), indices));
        return this;
    }

    public GetFieldMappingsRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    /** Sets the fields to retrieve. */
    public GetFieldMappingsRequestBuilder setFields(String... fields) {
        request.fields(fields);
        return this;
    }

    /** Indicates whether default mapping settings should be returned */
    public GetFieldMappingsRequestBuilder includeDefaults(boolean includeDefaults) {
        request.includeDefaults(includeDefaults);
        return this;
    }
}
