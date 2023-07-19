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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.mapping.get.GetFieldAliasesMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetFieldAliasesMappingsResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldAliasesMappingsResponse.FieldAliasesMappingMetadata;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.core.rest.RestStatus.NOT_FOUND;
import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get field aliases mapping
 *
 * @opensearch.api
 */
public class RestGetFieldAliasesMappingAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestGetFieldAliasesMappingAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(GET, "/_mapping/field/{fields}/aliases"), new Route(GET, "/{index}/_mapping/field/{fields}/aliases"))
        );
    }

    @Override
    public String getName() {
        return "get_field_aliases_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final String[] fields = Strings.splitStringByCommaToArray(request.param("fields"));

        GetFieldAliasesMappingsRequest getMappingsRequest = new GetFieldAliasesMappingsRequest();
        getMappingsRequest.indices(indices).fields(fields).includeDefaults(request.paramAsBoolean("include_defaults", false));
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));

        return channel -> client.admin()
            .indices()
            .getFieldAliasesMappings(getMappingsRequest, new RestBuilderListener<GetFieldAliasesMappingsResponse>(channel) {
                @Override
                public RestResponse buildResponse(GetFieldAliasesMappingsResponse response, XContentBuilder builder) throws Exception {
                    Map<String, Map<String, FieldAliasesMappingMetadata>> mappingsByIndex = response.mappings();

                    RestStatus status = OK;
                    if (mappingsByIndex.isEmpty() && fields.length > 0) {
                        status = NOT_FOUND;
                    }
                    response.toXContent(builder, request);
                    return new BytesRestResponse(status, builder);
                }
            });
    }

}
