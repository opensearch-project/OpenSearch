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

import org.opensearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Transport action to add index block
 *
 * @opensearch.api
 */
public class RestAddIndexBlockAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestAddIndexBlockAction.class);

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(PUT, "/{index}/_block/{block}"));
    }

    @Override
    public String getName() {
        return "add_index_block_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        AddIndexBlockRequest addIndexBlockRequest = new AddIndexBlockRequest(
            IndexMetadata.APIBlock.fromName(request.param("block")),
            Strings.splitStringByCommaToArray(request.param("index"))
        );
        addIndexBlockRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", addIndexBlockRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(addIndexBlockRequest, request, deprecationLogger, getName());
        addIndexBlockRequest.timeout(request.paramAsTime("timeout", addIndexBlockRequest.timeout()));
        addIndexBlockRequest.indicesOptions(IndicesOptions.fromRequest(request, addIndexBlockRequest.indicesOptions()));
        return channel -> client.admin().indices().addBlock(addIndexBlockRequest, new RestToXContentListener<>(channel));
    }

}
