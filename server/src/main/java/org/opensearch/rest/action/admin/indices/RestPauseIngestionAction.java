/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Transport action to pause pull-based ingestion.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RestPauseIngestionAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPauseIngestionAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/{index}/ingestion/_pause")));
    }

    @Override
    public String getName() {
        return "pause_ingestion_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        PauseIngestionRequest pauseIngestionRequest = new PauseIngestionRequest(Strings.splitStringByCommaToArray(request.param("index")));
        pauseIngestionRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", pauseIngestionRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(pauseIngestionRequest, request, deprecationLogger, getName());
        pauseIngestionRequest.timeout(request.paramAsTime("timeout", pauseIngestionRequest.timeout()));
        pauseIngestionRequest.indicesOptions(IndicesOptions.fromRequest(request, pauseIngestionRequest.indicesOptions()));

        return channel -> client.admin().indices().pauseIngestion(pauseIngestionRequest, new RestToXContentListener<>(channel));
    }

}
