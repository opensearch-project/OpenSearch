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

package org.opensearch.rest.action.cat;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.common.Table;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Locale;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _cat API action to get cluster health
 *
 * @opensearch.api
 */
public class RestHealthAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat/health"));
    }

    @Override
    public String getName() {
        return "cat_health_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/health\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest();

        return channel -> client.admin().cluster().health(clusterHealthRequest, new RestResponseListener<ClusterHealthResponse>(channel) {
            @Override
            public RestResponse buildResponse(final ClusterHealthResponse health) throws Exception {
                return RestTable.buildResponse(buildTable(health, request), channel);
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table t = new Table();
        t.startHeadersWithTimestamp();
        t.addCell("cluster", "alias:cl;desc:cluster name");
        t.addCell("status", "alias:st;desc:health status");
        t.addCell("node.total", "alias:nt,nodeTotal;text-align:right;desc:total number of nodes");
        t.addCell("node.data", "alias:nd,nodeData;text-align:right;desc:number of nodes that can store data");
        // TODO: Remove the header alias 'discovered_master', after removing MASTER_ROLE.
        // The alias 'discovered_master' is added for compatibility when using request parameter 'h=discovered_master'.
        t.addCell(
            "discovered_cluster_manager",
            "alias:dcm,dm,discovered_master;text-align:right;desc:cluster manager is discovered or not"
        );
        t.addCell("shards", "alias:t,sh,shards.total,shardsTotal;text-align:right;desc:total number of shards");
        t.addCell("pri", "alias:p,shards.primary,shardsPrimary;text-align:right;desc:number of primary shards");
        t.addCell("relo", "alias:r,shards.relocating,shardsRelocating;text-align:right;desc:number of relocating nodes");
        t.addCell("init", "alias:i,shards.initializing,shardsInitializing;text-align:right;desc:number of initializing nodes");
        t.addCell("unassign", "alias:u,shards.unassigned,shardsUnassigned;text-align:right;desc:number of unassigned shards");
        t.addCell("pending_tasks", "alias:pt,pendingTasks;text-align:right;desc:number of pending tasks");
        t.addCell("max_task_wait_time", "alias:mtwt,maxTaskWaitTime;text-align:right;desc:wait time of longest task pending");
        t.addCell("active_shards_percent", "alias:asp,activeShardsPercent;text-align:right;desc:active number of shards in percent");
        t.endHeaders();

        return t;
    }

    private Table buildTable(final ClusterHealthResponse health, final RestRequest request) {
        Table t = getTableWithHeader(request);
        t.startRow();
        t.addCell(health.getClusterName());
        t.addCell(health.getStatus().name().toLowerCase(Locale.ROOT));
        t.addCell(health.getNumberOfNodes());
        t.addCell(health.getNumberOfDataNodes());
        t.addCell(health.hasDiscoveredClusterManager());
        t.addCell(health.getActiveShards());
        t.addCell(health.getActivePrimaryShards());
        t.addCell(health.getRelocatingShards());
        t.addCell(health.getInitializingShards());
        t.addCell(health.getUnassignedShards());
        t.addCell(health.getNumberOfPendingTasks());
        t.addCell(health.getTaskMaxWaitingTime().millis() == 0 ? "-" : health.getTaskMaxWaitingTime());
        t.addCell(String.format(Locale.ROOT, "%1.1f%%", health.getActiveShardsPercent()));
        t.endRow();
        return t;
    }
}
