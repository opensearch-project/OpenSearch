/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.core.common.Strings.splitStringByCommaToArray;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Rest Tiering API action to move indices to warm tier
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class RestWarmTieringAction extends BaseRestHandler {

    private static final String TARGET_TIER = "warm";

    @Override
    public List<RestHandler.Route> routes() {
        return singletonList(new RestHandler.Route(POST, "/{index}/_tier/" + TARGET_TIER));
    }

    @Override
    public String getName() {
        return "warm_tiering_action";
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final TieringIndexRequest tieringIndexRequest = new TieringIndexRequest(
            TARGET_TIER,
            splitStringByCommaToArray(request.param("index"))
        );
        tieringIndexRequest.timeout(request.paramAsTime("timeout", tieringIndexRequest.timeout()));
        tieringIndexRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", tieringIndexRequest.clusterManagerNodeTimeout())
        );
        tieringIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, tieringIndexRequest.indicesOptions()));
        tieringIndexRequest.waitForCompletion(request.paramAsBoolean("wait_for_completion", tieringIndexRequest.waitForCompletion()));
        return channel -> client.admin()
            .cluster()
            .execute(HotToWarmTieringAction.INSTANCE, tieringIndexRequest, new RestToXContentListener<>(channel));
    }
}
