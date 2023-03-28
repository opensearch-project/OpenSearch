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

package org.opensearch.action.admin.indices.mapping.put;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.master.AcknowledgedRequestBuilder;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.Index;

import java.util.Map;

/**
 * Builder for a put mapping request
 *
 * @opensearch.internal
 */
public class PutMappingRequestBuilder extends AcknowledgedRequestBuilder<
    PutMappingRequest,
    AcknowledgedResponse,
    PutMappingRequestBuilder> {

    public PutMappingRequestBuilder(OpenSearchClient client, PutMappingAction action) {
        super(client, action, new PutMappingRequest());
    }

    public PutMappingRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public PutMappingRequestBuilder setConcreteIndex(Index index) {
        request.setConcreteIndex(index);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public PutMappingRequestBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    /**
     * The mapping source definition.
     */
    public PutMappingRequestBuilder setSource(XContentBuilder mappingBuilder) {
        request.source(mappingBuilder);
        return this;
    }

    /**
     * The mapping source definition.
     */
    public PutMappingRequestBuilder setSource(Map mappingSource) {
        request.source(mappingSource);
        return this;
    }

    /**
     * The mapping source definition.
     */
    public PutMappingRequestBuilder setSource(String mappingSource, XContentType xContentType) {
        request.source(mappingSource, xContentType);
        return this;
    }

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true").
     */
    public PutMappingRequestBuilder setSource(String... source) {
        request.source(source);
        return this;
    }

}
