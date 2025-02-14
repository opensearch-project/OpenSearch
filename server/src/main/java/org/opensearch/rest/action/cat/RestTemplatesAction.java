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

package org.opensearch.rest.action.cat;

import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.Table;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.regex.Regex;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _cat API action to get template information
 *
 * @opensearch.api
 */
public class RestTemplatesAction extends AbstractCatAction {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestTemplatesAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_cat/templates"), new Route(GET, "/_cat/templates/{name}")));
    }

    @Override
    public String getName() {
        return "cat_templates_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/templates\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, NodeClient client) {
        final String matchPattern = request.hasParam("name") ? request.param("name") : null;
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.clear().metadata(true);
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", clusterStateRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(clusterStateRequest, request, deprecationLogger, getName());

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestResponseListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse clusterStateResponse) throws Exception {
                return RestTable.buildResponse(buildTable(request, clusterStateResponse, matchPattern), channel);
            }
        });
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("name", "alias:n;desc:template name");
        table.addCell("index_patterns", "alias:t;desc:template index patterns");
        table.addCell("order", "alias:o,p;desc:template application order/priority number");
        table.addCell("version", "alias:v;desc:version");
        table.addCell("composed_of", "alias:c;desc:component templates comprising index template");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, ClusterStateResponse clusterStateResponse, String patternString) {
        Table table = getTableWithHeader(request);
        Metadata metadata = clusterStateResponse.getState().metadata();
        for (final IndexTemplateMetadata indexData : metadata.templates().values()) {
            if (patternString == null || Regex.simpleMatch(patternString, indexData.name())) {
                table.startRow();
                table.addCell(indexData.name());
                table.addCell("[" + String.join(", ", indexData.patterns()) + "]");
                table.addCell(indexData.getOrder());
                table.addCell(indexData.getVersion());
                table.addCell("");
                table.endRow();
            }
        }

        for (Map.Entry<String, ComposableIndexTemplate> entry : metadata.templatesV2().entrySet()) {
            String name = entry.getKey();
            ComposableIndexTemplate template = entry.getValue();
            if (patternString == null || Regex.simpleMatch(patternString, name)) {
                table.startRow();
                table.addCell(name);
                table.addCell("[" + String.join(", ", template.indexPatterns()) + "]");
                table.addCell(template.priorityOrZero());
                table.addCell(template.version());
                table.addCell("[" + String.join(", ", template.composedOf()) + "]");
                table.endRow();
            }
        }
        return table;
    }
}
