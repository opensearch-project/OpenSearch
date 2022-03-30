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
import org.opensearch.common.Strings;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.xcontent.XContentHelper;
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

public class RestPutMappingAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPutMappingAction.class);
    private static final String MASTER_TIMEOUT_DEPRECATED_MESSAGE =
        "Deprecated parameter [master_timeout] used. To promote inclusive language, please use [cluster_manager_timeout] instead. It will be unsupported in a future major version.";

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
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false, request.getXContentType()).v2();

        if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, sourceAsMap)) {
            throw new IllegalArgumentException("Types cannot be provided in put mapping requests");
        }

        putMappingRequest.source(sourceAsMap);
        putMappingRequest.timeout(request.paramAsTime("timeout", putMappingRequest.timeout()));
        putMappingRequest.masterNodeTimeout(request.paramAsTime("cluster_manager_timeout", putMappingRequest.masterNodeTimeout()));
        parseDeprecatedMasterTimeoutParameter(putMappingRequest, request);
        putMappingRequest.indicesOptions(IndicesOptions.fromRequest(request, putMappingRequest.indicesOptions()));
        putMappingRequest.writeIndexOnly(request.paramAsBoolean("write_index_only", false));
        return channel -> client.admin().indices().putMapping(putMappingRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Parse the deprecated request parameter 'master_timeout', and add deprecated log if the parameter is used.
     * It also validates whether the value of 'master_timeout' is the same with 'cluster_manager_timeout'.
     * Remove the method along with MASTER_ROLE.
     * @deprecated As of 2.0, because promoting inclusive language.
     */
    @Deprecated
    private void parseDeprecatedMasterTimeoutParameter(PutMappingRequest putMappingRequest, RestRequest request) {
        final String deprecatedTimeoutParam = "master_timeout";
        if (request.hasParam(deprecatedTimeoutParam)) {
            deprecationLogger.deprecate("put_mapping_master_timeout_parameter", MASTER_TIMEOUT_DEPRECATED_MESSAGE);
            request.validateParamValuesAreEqual(deprecatedTimeoutParam, "cluster_manager_timeout");
            putMappingRequest.masterNodeTimeout(request.paramAsTime(deprecatedTimeoutParam, putMappingRequest.masterNodeTimeout()));
        }
    }
}
