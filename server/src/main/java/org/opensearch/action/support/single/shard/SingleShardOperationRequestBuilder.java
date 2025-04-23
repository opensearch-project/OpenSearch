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

package org.opensearch.action.support.single.shard;

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.ActionType;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * Request builder for a single shard operation request
 *
 * @opensearch.internal
 */
public abstract class SingleShardOperationRequestBuilder<
    Request extends SingleShardRequest<Request>,
    Response extends ActionResponse,
    RequestBuilder extends SingleShardOperationRequestBuilder<Request, Response, RequestBuilder>> extends ActionRequestBuilder<
        Request,
        Response> {

    protected SingleShardOperationRequestBuilder(OpenSearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }

    /**
     * Sets the index.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setIndex(String index) {
        request.index(index);
        return (RequestBuilder) this;
    }
}
