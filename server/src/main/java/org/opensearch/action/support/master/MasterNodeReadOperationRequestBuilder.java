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

package org.opensearch.action.support.master;

import org.opensearch.action.ActionType;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.client.OpenSearchClient;

/**
 * Base request builder for cluster-manager node read operations that can be executed on the local node as well
 *
 * @opensearch.internal
 * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link ClusterManagerNodeReadOperationRequestBuilder}
 */
@Deprecated
public abstract class MasterNodeReadOperationRequestBuilder<
    Request extends ClusterManagerNodeReadRequest<Request>,
    Response extends ActionResponse,
    RequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<Request, Response, RequestBuilder>> extends
    ClusterManagerNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

    protected MasterNodeReadOperationRequestBuilder(OpenSearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }
}
