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

import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.Strings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.client.Requests.putMappingRequest;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Transport action to put mapping
 *
 * @opensearch.api
 */
public class RestPutMappingAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPutMappingAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(POST, "/{index}/_mapping/"),
                new Route(PUT, "/{index}/_mapping/"),
                new Route(POST, "/{index}/_mappings/"),
                new Route(PUT, "/{index}/_mappings/")
            )
        );
    }

    @Override
    public String getName() {
        return "put_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {

        PutMappingRequest putMappingRequest = putMappingRequest(Strings.splitStringByCommaToArray(request.param("index")));
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false, request.getMediaType()).v2();

        if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, sourceAsMap)) {
            throw new IllegalArgumentException("Types cannot be provided in put mapping requests");
        }

        putMappingRequest.source(sourceAsMap);
        putMappingRequest.timeout(request.paramAsTime("timeout", putMappingRequest.timeout()));
        putMappingRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", putMappingRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(putMappingRequest, request, deprecationLogger, getName());
        putMappingRequest.indicesOptions(IndicesOptions.fromRequest(request, putMappingRequest.indicesOptions()));
        putMappingRequest.writeIndexOnly(request.paramAsBoolean("write_index_only", false));
        return channel -> client.admin().indices().putMapping(putMappingRequest, new RestToXContentListener<>(channel));
    }
}
