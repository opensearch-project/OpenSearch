/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.wlm.QueryGroupStatsRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get QueryGroup stats
 *
 * @opensearch.experimental
 */
public class RestQueryGroupStatsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
<<<<<<< HEAD
                new Route(GET, "_wlm/stats"),
                new Route(GET, "_wlm/{nodeId}/stats"),
                new Route(GET, "_wlm/stats/{queryGroupId}"),
                new Route(GET, "_wlm/{nodeId}/stats/{queryGroupId}")
=======
                new Route(GET, "query_group/stats"),
                new Route(GET, "query_group/stats/{queryGroupId}"),
                new Route(GET, "query_group/stats/nodes/{nodeId}"),
                new Route(GET, "query_group/stats/{queryGroupId}/nodes/{nodeId}")
>>>>>>> ffe0d7fa2cd (address comments)
            )
        );
    }

    @Override
    public String getName() {
        return "query_group_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        Set<String> queryGroupIds = Strings.tokenizeByCommaToSet(request.param("queryGroupId", "_all"));
        Boolean breach = request.hasParam("breach") ? Boolean.parseBoolean(request.param("boolean")) : null;
        QueryGroupStatsRequest queryGroupStatsRequest = new QueryGroupStatsRequest(nodesIds, queryGroupIds, breach);
        return channel -> client.admin()
            .cluster()
            .queryGroupStats(queryGroupStatsRequest, new RestActions.NodesResponseRestListener<>(channel));
    }
}
