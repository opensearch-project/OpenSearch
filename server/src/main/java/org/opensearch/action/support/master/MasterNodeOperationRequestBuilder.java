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
import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;

/**
 * Base request builder for cluster-manager node operations
 *
 * @opensearch.internal
 * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link ClusterManagerNodeOperationRequestBuilder}
 */
@Deprecated
public abstract class MasterNodeOperationRequestBuilder<
    Request extends MasterNodeRequest<Request>,
    Response extends ActionResponse,
    RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder>> extends
    ClusterManagerNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

    protected MasterNodeOperationRequestBuilder(OpenSearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }
}
