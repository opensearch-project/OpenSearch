/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.master;

import org.opensearch.action.ActionType;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;

/**
 * Base request builder for cluster-manager node read operations that can be executed on the local node as well
 *
 * @opensearch.internal
 * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link ClusterManagerNodeReadOperationRequestBuilder}
 */
@Deprecated
public abstract class MasterNodeReadOperationRequestBuilder<
    Request extends MasterNodeReadRequest<Request>,
    Response extends ActionResponse,
    RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder>> extends
    ClusterManagerNodeReadOperationRequestBuilder<Request, Response, RequestBuilder> {

    protected MasterNodeReadOperationRequestBuilder(OpenSearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }
}
