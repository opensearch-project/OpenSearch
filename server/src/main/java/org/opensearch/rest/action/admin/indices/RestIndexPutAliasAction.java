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
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Transport action to put alias(es)
 *
 * @opensearch.api
 */
public class RestIndexPutAliasAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestIndexPutAliasAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(POST, "/{index}/_alias/{name}"),
                new Route(PUT, "/{index}/_alias/{name}"),
                new Route(POST, "/_alias/{name}"),
                new Route(PUT, "/_alias/{name}"),
                new Route(POST, "/{index}/_aliases/{name}"),
                new Route(PUT, "/{index}/_aliases/{name}"),
                new Route(POST, "/_aliases/{name}"),
                new Route(PUT, "/_aliases/{name}"),
                new Route(PUT, "/{index}/_alias"),
                new Route(PUT, "/{index}/_aliases"),
                new Route(PUT, "/_alias")
            )
        );
    }

    @Override
    public String getName() {
        return "index_put_alias_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String alias = request.param("name");
        Map<String, Object> filter = null;
        String routing = null;
        String indexRouting = null;
        String searchRouting = null;
        Boolean writeIndex = null;
        Boolean isHidden = null;

        if (request.hasContent()) {
            try (XContentParser parser = request.contentParser()) {
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    throw new IllegalArgumentException("No index alias is specified");
                }
                String currentFieldName = null;
                while ((token = parser.nextToken()) != null) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("index".equals(currentFieldName)) {
                            indices = Strings.splitStringByCommaToArray(parser.text());
                        } else if ("alias".equals(currentFieldName)) {
                            alias = parser.text();
                        } else if ("routing".equals(currentFieldName)) {
                            routing = parser.textOrNull();
                        } else if ("indexRouting".equals(currentFieldName)
                            || "index-routing".equals(currentFieldName)
                            || "index_routing".equals(currentFieldName)) {
                                indexRouting = parser.textOrNull();
                            } else if ("searchRouting".equals(currentFieldName)
                                || "search-routing".equals(currentFieldName)
                                || "search_routing".equals(currentFieldName)) {
                                    searchRouting = parser.textOrNull();
                                } else if ("is_write_index".equals(currentFieldName)) {
                                    writeIndex = parser.booleanValue();
                                } else if ("is_hidden".equals(currentFieldName)) {
                                    isHidden = parser.booleanValue();
                                } else {
                                    throw new IllegalArgumentException("unknown field [" + currentFieldName + "]");
                                }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if ("filter".equals(currentFieldName)) {
                            filter = parser.mapOrdered();
                        } else {
                            throw new IllegalArgumentException("unknown field [" + currentFieldName + "]");
                        }
                    }
                }
            }
        }

        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.timeout(request.paramAsTime("timeout", indicesAliasesRequest.timeout()));
        indicesAliasesRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", indicesAliasesRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(indicesAliasesRequest, request, deprecationLogger, getName());

        IndicesAliasesRequest.AliasActions aliasAction = AliasActions.add().indices(indices).alias(alias);
        if (routing != null) {
            aliasAction.routing(routing);
        }
        if (searchRouting != null) {
            aliasAction.searchRouting(searchRouting);
        }
        if (indexRouting != null) {
            aliasAction.indexRouting(indexRouting);
        }
        if (filter != null) {
            aliasAction.filter(filter);
        }
        if (writeIndex != null) {
            aliasAction.writeIndex(writeIndex);
        }
        if (isHidden != null) {
            aliasAction.isHidden(isHidden);
        }
        indicesAliasesRequest.addAliasAction(aliasAction);
        return channel -> client.admin().indices().aliases(indicesAliasesRequest, new RestToXContentListener<>(channel));
    }
}
