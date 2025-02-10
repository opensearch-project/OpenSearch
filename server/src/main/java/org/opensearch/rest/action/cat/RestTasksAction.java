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

import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.TaskGroup;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Table;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.transport.client.node.NodeClient;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.action.admin.cluster.RestListTasksAction.generateListTasksRequest;

/**
 * _cat API action to get tasks information
 *
 * @opensearch.api
 */
public class RestTasksAction extends AbstractCatAction {
    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestTasksAction(Supplier<DiscoveryNodes> nodesInCluster) {
        this.nodesInCluster = nodesInCluster;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat/tasks"));
    }

    @Override
    public String getName() {
        return "cat_tasks_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/tasks\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final ListTasksRequest listTasksRequest = generateListTasksRequest(request);
        return channel -> client.admin().cluster().listTasks(listTasksRequest, new RestResponseListener<ListTasksResponse>(channel) {
            @Override
            public RestResponse buildResponse(ListTasksResponse listTasksResponse) throws Exception {
                return RestTable.buildResponse(buildTable(request, listTasksResponse), channel);
            }
        });
    }

    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>();
        responseParams.add("detailed");
        responseParams.addAll(AbstractCatAction.RESPONSE_PARAMS);
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        boolean detailed = request.paramAsBoolean("detailed", false);
        Table table = new Table();
        table.startHeaders();

        // Task main info
        table.addCell("id", "default:false;desc:id of the task with the node");
        table.addCell("action", "alias:ac;desc:task action");
        table.addCell("task_id", "alias:ti;desc:unique task id");
        table.addCell("parent_task_id", "alias:pti;desc:parent task id");
        table.addCell("type", "alias:ty;desc:task type");
        table.addCell("start_time", "alias:start;desc:start time in ms");
        table.addCell("timestamp", "alias:ts,hms,hhmmss;desc:start time in HH:MM:SS");
        table.addCell("running_time_ns", "default:false;alias:time;desc:running time ns");
        table.addCell("running_time", "default:true;alias:time;desc:running time");

        // Node info
        table.addCell("node_id", "default:false;alias:ni;desc:unique node id");
        table.addCell("ip", "default:true;alias:i;desc:ip address");
        table.addCell("port", "default:false;alias:po;desc:bound transport port");
        table.addCell("node", "default:true;alias:n;desc:node name");
        table.addCell("version", "default:false;alias:v;desc:es version");
        table.addCell("x_opaque_id", "default:false;alias:x;desc:X-Opaque-ID header");

        // Task detailed info
        if (detailed) {
            table.addCell("description", "default:true;alias:desc;desc:task action");
            table.addCell("resource_stats", "default:false;desc:resource consumption info of the task");
        }
        table.endHeaders();
        return table;
    }

    private static final DateFormatter FORMATTER = DateFormatter.forPattern("HH:mm:ss").withZone(ZoneOffset.UTC);

    private void buildRow(Table table, boolean fullId, boolean detailed, DiscoveryNodes discoveryNodes, TaskInfo taskInfo) {
        table.startRow();
        String nodeId = taskInfo.getTaskId().getNodeId();
        DiscoveryNode node = discoveryNodes.get(nodeId);

        table.addCell(taskInfo.getId());
        table.addCell(taskInfo.getAction());
        table.addCell(taskInfo.getTaskId().toString());
        if (taskInfo.getParentTaskId().isSet()) {
            table.addCell(taskInfo.getParentTaskId().toString());
        } else {
            table.addCell("-");
        }
        table.addCell(taskInfo.getType());
        table.addCell(taskInfo.getStartTime());
        table.addCell(FORMATTER.format(Instant.ofEpochMilli(taskInfo.getStartTime())));
        table.addCell(taskInfo.getRunningTimeNanos());
        table.addCell(TimeValue.timeValueNanos(taskInfo.getRunningTimeNanos()));

        // Node information. Note that the node may be null because it has left the cluster between when we got this response and now.
        table.addCell(fullId ? nodeId : Strings.substring(nodeId, 0, 4));
        table.addCell(node == null ? "-" : node.getHostAddress());
        table.addCell(node.getAddress().address().getPort());
        table.addCell(node == null ? "-" : node.getName());
        table.addCell(node == null ? "-" : node.getVersion().toString());
        table.addCell(taskInfo.getHeaders().getOrDefault(Task.X_OPAQUE_ID, "-"));

        if (detailed) {
            table.addCell(taskInfo.getDescription());
            table.addCell(taskInfo.getResourceStats());
        }
        table.endRow();
    }

    private void buildGroups(Table table, boolean fullId, boolean detailed, List<TaskGroup> taskGroups) {
        DiscoveryNodes discoveryNodes = nodesInCluster.get();
        List<TaskGroup> sortedGroups = new ArrayList<>(taskGroups);
        sortedGroups.sort(Comparator.comparingLong(o -> o.getTaskInfo().getStartTime()));
        for (TaskGroup taskGroup : sortedGroups) {
            buildRow(table, fullId, detailed, discoveryNodes, taskGroup.getTaskInfo());
            buildGroups(table, fullId, detailed, taskGroup.getChildTasks());
        }
    }

    private Table buildTable(RestRequest request, ListTasksResponse listTasksResponse) {
        boolean fullId = request.paramAsBoolean("full_id", false);
        boolean detailed = request.paramAsBoolean("detailed", false);
        Table table = getTableWithHeader(request);
        buildGroups(table, fullId, detailed, listTasksResponse.getTaskGroups());
        return table;
    }
}
