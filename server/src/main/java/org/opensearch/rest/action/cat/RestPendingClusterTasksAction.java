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

import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.PendingClusterTask;
import org.opensearch.common.Table;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _cat API action to get pending cluster tasks
 *
 * @opensearch.api
 */
public class RestPendingClusterTasksAction extends AbstractCatAction {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPendingClusterTasksAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat/pending_tasks"));
    }

    @Override
    public String getName() {
        return "cat_pending_cluster_tasks_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/pending_tasks\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        PendingClusterTasksRequest pendingClusterTasksRequest = new PendingClusterTasksRequest();
        pendingClusterTasksRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", pendingClusterTasksRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(pendingClusterTasksRequest, request, deprecationLogger, getName());
        pendingClusterTasksRequest.local(request.paramAsBoolean("local", pendingClusterTasksRequest.local()));
        return channel -> client.admin()
            .cluster()
            .pendingClusterTasks(pendingClusterTasksRequest, new RestResponseListener<PendingClusterTasksResponse>(channel) {
                @Override
                public RestResponse buildResponse(PendingClusterTasksResponse pendingClusterTasks) throws Exception {
                    Table tab = buildTable(request, pendingClusterTasks);
                    return RestTable.buildResponse(tab, channel);
                }
            });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table t = new Table();
        t.startHeaders();
        t.addCell("insertOrder", "alias:o;text-align:right;desc:task insertion order");
        t.addCell("timeInQueue", "alias:t;text-align:right;desc:how long task has been in queue");
        t.addCell("priority", "alias:p;desc:task priority");
        t.addCell("source", "alias:s;desc:task source");
        t.endHeaders();
        return t;
    }

    private Table buildTable(RestRequest request, PendingClusterTasksResponse tasks) {
        Table t = getTableWithHeader(request);

        for (PendingClusterTask task : tasks) {
            t.startRow();
            t.addCell(task.getInsertOrder());
            t.addCell(task.getTimeInQueue());
            t.addCell(task.getPriority());
            t.addCell(task.getSource());
            t.endRow();
        }

        return t;
    }
}
