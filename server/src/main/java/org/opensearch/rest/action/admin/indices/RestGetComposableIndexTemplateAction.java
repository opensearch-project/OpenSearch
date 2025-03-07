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

import org.opensearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.opensearch.core.rest.RestStatus.NOT_FOUND;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.HEAD;

/**
 * Transport action to get composable index template
 *
 * @opensearch.api
 */
public class RestGetComposableIndexTemplateAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetComposableIndexTemplateAction.class);

    @Override
    public List<Route> routes() {
        return Arrays.asList(
            new Route(GET, "/_index_template"),
            new Route(GET, "/_index_template/{name}"),
            new Route(HEAD, "/_index_template/{name}")
        );
    }

    @Override
    public String getName() {
        return "get_composable_index_template_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final GetComposableIndexTemplateAction.Request getRequest = new GetComposableIndexTemplateAction.Request(request.param("name"));

        getRequest.local(request.paramAsBoolean("local", getRequest.local()));
        getRequest.clusterManagerNodeTimeout(request.paramAsTime("cluster_manager_timeout", getRequest.clusterManagerNodeTimeout()));
        parseDeprecatedMasterTimeoutParameter(getRequest, request, deprecationLogger, getName());

        final boolean implicitAll = getRequest.name() == null;

        return channel -> client.execute(
            GetComposableIndexTemplateAction.INSTANCE,
            getRequest,
            new RestToXContentListener<GetComposableIndexTemplateAction.Response>(channel) {
                @Override
                protected RestStatus getStatus(final GetComposableIndexTemplateAction.Response response) {
                    final boolean templateExists = response.indexTemplates().isEmpty() == false;
                    return (templateExists || implicitAll) ? OK : NOT_FOUND;
                }
            }
        );
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

}
