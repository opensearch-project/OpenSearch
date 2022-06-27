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

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Transport action to create an index
 *
 * @opensearch.api
 */
public class RestCreateIndexAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestIndexPutAliasAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(PUT, "/{index}"));
    }

    @Override
    public String getName() {
        return "create_index_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(request.param("index"));

        if (request.hasContent()) {
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(request.requiredContent(), false, request.getXContentType()).v2();
            sourceAsMap = prepareMappings(sourceAsMap);
            createIndexRequest.source(sourceAsMap, LoggingDeprecationHandler.INSTANCE);
        }

        createIndexRequest.timeout(request.paramAsTime("timeout", createIndexRequest.timeout()));
        createIndexRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", createIndexRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(createIndexRequest, request, deprecationLogger, getName());
        createIndexRequest.waitForActiveShards(ActiveShardCount.parseString(request.param("wait_for_active_shards")));
        return channel -> client.admin().indices().create(createIndexRequest, new RestToXContentListener<>(channel));
    }

    static Map<String, Object> prepareMappings(Map<String, Object> source) {
        if (source.containsKey("mappings") == false || (source.get("mappings") instanceof Map) == false) {
            return source;
        }

        Map<String, Object> newSource = new HashMap<>(source);

        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) source.get("mappings");
        if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, mappings)) {
            throw new IllegalArgumentException("The mapping definition cannot be nested under a type");
        }

        newSource.put("mappings", singletonMap(MapperService.SINGLE_MAPPING_NAME, mappings));
        return newSource;
    }
}
