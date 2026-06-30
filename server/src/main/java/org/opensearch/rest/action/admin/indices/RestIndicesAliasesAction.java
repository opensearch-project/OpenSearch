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

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Transport action to list alias(es)
 *
 * @opensearch.api
 */
public class RestIndicesAliasesAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestIndicesAliasesAction.class);

    @Override
    public String getName() {
        return "indices_aliases_action";
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_aliases"));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", indicesAliasesRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(indicesAliasesRequest, request, deprecationLogger, getName());
        indicesAliasesRequest.timeout(request.paramAsTime("timeout", indicesAliasesRequest.timeout()));
        try (XContentParser parser = request.contentParser()) {
            IndicesAliasesRequest.PARSER.parse(parser, indicesAliasesRequest, null);
        }
        if (indicesAliasesRequest.getAliasActions().isEmpty()) {
            throw new IllegalArgumentException("No action specified");
        }
        return channel -> client.admin().indices().aliases(indicesAliasesRequest, new RestToXContentListener<>(channel));
    }
}
