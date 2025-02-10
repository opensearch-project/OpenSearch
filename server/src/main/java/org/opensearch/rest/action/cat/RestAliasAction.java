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

import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.Table;
import org.opensearch.core.common.Strings;
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
 * _cat API action to list aliases
 *
 * @opensearch.api
 */
public class RestAliasAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_cat/aliases"), new Route(GET, "/_cat/aliases/{alias}")));
    }

    @Override
    public String getName() {
        return "cat_alias_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final GetAliasesRequest getAliasesRequest = request.hasParam("alias")
            ? new GetAliasesRequest(Strings.commaDelimitedListToStringArray(request.param("alias")))
            : new GetAliasesRequest();
        getAliasesRequest.indicesOptions(IndicesOptions.fromRequest(request, getAliasesRequest.indicesOptions()));
        getAliasesRequest.local(request.paramAsBoolean("local", getAliasesRequest.local()));

        return channel -> client.admin().indices().getAliases(getAliasesRequest, new RestResponseListener<GetAliasesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetAliasesResponse response) throws Exception {
                Table tab = buildTable(request, response);
                return RestTable.buildResponse(tab, channel);
            }
        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/aliases\n");
        sb.append("/_cat/aliases/{alias}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        final Table table = new Table();
        table.startHeaders();
        table.addCell("alias", "alias:a;desc:alias name");
        table.addCell("index", "alias:i,idx;desc:index alias points to");
        table.addCell("filter", "alias:f,fi;desc:filter");
        table.addCell("routing.index", "alias:ri,routingIndex;desc:index routing");
        table.addCell("routing.search", "alias:rs,routingSearch;desc:search routing");
        table.addCell("is_write_index", "alias:w,isWriteIndex;desc:write index");
        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, GetAliasesResponse response) {
        Table table = getTableWithHeader(request);

        for (final Map.Entry<String, List<AliasMetadata>> cursor : response.getAliases().entrySet()) {
            String indexName = cursor.getKey();
            for (AliasMetadata aliasMetadata : cursor.getValue()) {
                table.startRow();
                table.addCell(aliasMetadata.alias());
                table.addCell(indexName);
                table.addCell(aliasMetadata.filteringRequired() ? "*" : "-");
                String indexRouting = Strings.hasLength(aliasMetadata.indexRouting()) ? aliasMetadata.indexRouting() : "-";
                table.addCell(indexRouting);
                String searchRouting = Strings.hasLength(aliasMetadata.searchRouting()) ? aliasMetadata.searchRouting() : "-";
                table.addCell(searchRouting);
                String isWriteIndex = aliasMetadata.writeIndex() == null ? "-" : aliasMetadata.writeIndex().toString();
                table.addCell(isWriteIndex);
                table.endRow();
            }
        }

        return table;
    }

}
