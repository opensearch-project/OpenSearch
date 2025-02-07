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

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequest;
import org.opensearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequestBuilder;
import org.opensearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsResponse;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestRequestFilter;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestActions;
import org.opensearch.rest.action.RestBuilderListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Transport action to reload secure settings
 *
 * @opensearch.api
 */
public final class RestReloadSecureSettingsAction extends BaseRestHandler implements RestRequestFilter {

    static final ObjectParser<NodesReloadSecureSettingsRequest, String> PARSER = new ObjectParser<>(
        "reload_secure_settings",
        NodesReloadSecureSettingsRequest::new
    );

    static {
        PARSER.declareString(
            (request, value) -> request.setSecureStorePassword(new SecureString(value.toCharArray())),
            new ParseField("secure_settings_password")
        );
    }

    @Override
    public String getName() {
        return "nodes_reload_action";
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(POST, "/_nodes/reload_secure_settings"), new Route(POST, "/_nodes/{nodeId}/reload_secure_settings"))
        );
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        final NodesReloadSecureSettingsRequestBuilder nodesRequestBuilder = client.admin()
            .cluster()
            .prepareReloadSecureSettings()
            .setTimeout(request.param("timeout"))
            .setNodesIds(nodesIds);
        request.withContentOrSourceParamParserOrNull(parser -> {
            if (parser != null) {
                final NodesReloadSecureSettingsRequest nodesRequest = PARSER.parse(parser, null);
                nodesRequestBuilder.setSecureStorePassword(nodesRequest.getSecureSettingsPassword());
            }
        });

        return channel -> nodesRequestBuilder.execute(new RestBuilderListener<NodesReloadSecureSettingsResponse>(channel) {
            @Override
            public RestResponse buildResponse(NodesReloadSecureSettingsResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                RestActions.buildNodesHeader(builder, channel.request(), response);
                builder.field("cluster_name", response.getClusterName().value());
                response.toXContent(builder, channel.request());
                builder.endObject();
                nodesRequestBuilder.request().closePassword();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    private static final Set<String> FILTERED_FIELDS = Collections.singleton("secure_settings_password");

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }
}
