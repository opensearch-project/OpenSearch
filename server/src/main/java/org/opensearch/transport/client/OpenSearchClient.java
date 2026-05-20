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

package org.opensearch.transport.client;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.threadpool.ThreadPool;

/**
 * Interface for an OpenSearch client implementation
 *
 * @opensearch.internal
 */
public interface OpenSearchClient {

    /**
     * Executes a generic action, denoted by an {@link ActionType}.
     *
     * @param action           The action type to execute.
     * @param request          The action request.
     * @param <Request>        The request type.
     * @param <Response>       the response type.
     * @return A future allowing to get back the response.
     */
    <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
        ActionType<Response> action,
        Request request
    );

    /**
     * Executes a generic action, denoted by an {@link ActionType}.
     *
     * @param action           The action type to execute.
     * @param request          The action request.
     * @param listener         The listener to receive the response back.
     * @param <Request>        The request type.
     * @param <Response>       The response type.
     */
    <Request extends ActionRequest, Response extends ActionResponse> void execute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    );

    /**
     * Returns the threadpool used to execute requests on this client
     */
    ThreadPool threadPool();

}
