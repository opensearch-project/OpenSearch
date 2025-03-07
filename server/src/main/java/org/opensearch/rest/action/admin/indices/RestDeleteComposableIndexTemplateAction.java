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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * Transport action to delete composable index template
 *
 * @opensearch.api
 */
public class RestDeleteComposableIndexTemplateAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestDeleteComposableIndexTemplateAction.class);

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(DELETE, "/_index_template/{name}"));
    }

    @Override
    public String getName() {
        return "delete_composable_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        DeleteComposableIndexTemplateAction.Request deleteReq = new DeleteComposableIndexTemplateAction.Request(request.param("name"));
        deleteReq.clusterManagerNodeTimeout(request.paramAsTime("cluster_manager_timeout", deleteReq.clusterManagerNodeTimeout()));
        parseDeprecatedMasterTimeoutParameter(deleteReq, request, deprecationLogger, getName());

        return channel -> client.execute(DeleteComposableIndexTemplateAction.INSTANCE, deleteReq, new RestToXContentListener<>(channel));
    }
}
