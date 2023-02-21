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

import org.opensearch.OpenSearchParseException;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestActionListener;
import org.opensearch.rest.action.RestBuilderListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get mapping
 *
 * @opensearch.api
 */
public class RestGetMappingAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetMappingAction.class);
    private static final String MASTER_TIMEOUT_DEPRECATED_MESSAGE =
        "Parameter [master_timeout] is deprecated and will be removed in 3.0. To support inclusive language, please use [cluster_manager_timeout] instead.";
    private static final String DUPLICATE_PARAMETER_ERROR_MESSAGE =
        "Please only use one of the request parameters [master_timeout, cluster_manager_timeout].";

    private final ThreadPool threadPool;

    public RestGetMappingAction(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_mapping"),
                new Route(GET, "/_mappings"),
                new Route(GET, "/{index}/_mapping"),
                new Route(GET, "/{index}/_mappings")
            )
        );
    }

    @Override
    public String getName() {
        return "get_mapping_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));

        final GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
        getMappingsRequest.indices(indices);
        getMappingsRequest.indicesOptions(IndicesOptions.fromRequest(request, getMappingsRequest.indicesOptions()));
        TimeValue clusterManagerTimeout = request.paramAsTime("cluster_manager_timeout", getMappingsRequest.clusterManagerNodeTimeout());
        // TODO: Remove the if condition and statements inside after removing MASTER_ROLE.
        if (request.hasParam("master_timeout")) {
            deprecationLogger.deprecate("get_mapping_master_timeout_parameter", MASTER_TIMEOUT_DEPRECATED_MESSAGE);
            if (request.hasParam("cluster_manager_timeout")) {
                throw new OpenSearchParseException(DUPLICATE_PARAMETER_ERROR_MESSAGE);
            }
            clusterManagerTimeout = request.paramAsTime("master_timeout", getMappingsRequest.clusterManagerNodeTimeout());
        }
        final TimeValue timeout = clusterManagerTimeout;
        getMappingsRequest.clusterManagerNodeTimeout(timeout);
        getMappingsRequest.local(request.paramAsBoolean("local", getMappingsRequest.local()));
        return channel -> client.admin().indices().getMappings(getMappingsRequest, new RestActionListener<GetMappingsResponse>(channel) {

            @Override
            protected void processResponse(GetMappingsResponse getMappingsResponse) {
                final long startTimeMs = threadPool.relativeTimeInMillis();
                // Process serialization on GENERIC pool since the serialization of the raw mappings to XContent can be too slow to execute
                // on an IO thread
                threadPool.executor(ThreadPool.Names.MANAGEMENT)
                    .execute(ActionRunnable.wrap(this, l -> new RestBuilderListener<GetMappingsResponse>(channel) {
                        @Override
                        public RestResponse buildResponse(final GetMappingsResponse response, final XContentBuilder builder)
                            throws Exception {
                            if (threadPool.relativeTimeInMillis() - startTimeMs > timeout.millis()) {
                                throw new OpenSearchTimeoutException("Timed out getting mappings");
                            }
                            builder.startObject();
                            response.toXContent(builder, request);
                            builder.endObject();
                            return new BytesRestResponse(RestStatus.OK, builder);
                        }
                    }.onResponse(getMappingsResponse)));
            }
        });
    }
}
