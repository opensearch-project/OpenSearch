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

package org.opensearch.rest.action.ingest;

import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Transport action to put a pipeline
 *
 * @opensearch.api
 */
public class RestPutPipelineAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPutPipelineAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(PUT, "/_ingest/pipeline/{id}"));
    }

    @Override
    public String getName() {
        return "ingest_put_pipeline_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        Tuple<MediaType, BytesReference> sourceTuple = restRequest.contentOrSourceParam();
        PutPipelineRequest request = new PutPipelineRequest(restRequest.param("id"), sourceTuple.v2(), sourceTuple.v1());
        request.clusterManagerNodeTimeout(restRequest.paramAsTime("cluster_manager_timeout", request.clusterManagerNodeTimeout()));
        parseDeprecatedMasterTimeoutParameter(request, restRequest, deprecationLogger, getName());
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        return channel -> client.admin().cluster().putPipeline(request, new RestToXContentListener<>(channel));
    }

}
