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

package org.opensearch.rest.action.admin.cluster.dangling;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.core.rest.RestStatus.ACCEPTED;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.admin.indices.dangling.import_index.ImportDanglingIndexRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.action.RestToXContentListener;

/**
 * Transport action to import dangling index
 *
 * @opensearch.api
 */
public class RestImportDanglingIndexAction extends BaseRestHandler {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestImportDanglingIndexAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_dangling/{index_uuid}"));
    }

    @Override
    public String getName() {
        return "import_dangling_index";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        final ImportDanglingIndexRequest importRequest = new ImportDanglingIndexRequest(
            request.param("index_uuid"),
            request.paramAsBoolean("accept_data_loss", false)
        );

        importRequest.timeout(request.paramAsTime("timeout", importRequest.timeout()));
        importRequest.clusterManagerNodeTimeout(request.paramAsTime("cluster_manager_timeout", importRequest.clusterManagerNodeTimeout()));
        parseDeprecatedMasterTimeoutParameter(importRequest, request, deprecationLogger, getName());

        return channel -> client.admin()
            .cluster()
            .importDanglingIndex(importRequest, new RestToXContentListener<AcknowledgedResponse>(channel) {
                @Override
                protected RestStatus getStatus(AcknowledgedResponse acknowledgedResponse) {
                    return ACCEPTED;
                }
            });
    }
}
