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

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.Strings;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestBuilderListener;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get cluster state
 *
 * @opensearch.api
 */
public class RestClusterStateAction extends BaseRestHandler {

    private final SettingsFilter settingsFilter;

    public RestClusterStateAction(SettingsFilter settingsFilter) {
        this.settingsFilter = settingsFilter;
    }

    // TODO: Remove the DeprecationLogger after removing MASTER_ROLE.
    // It's used to log deprecation when request parameter 'metric' contains 'master_node', or request parameter 'master_timeout' is used.
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestClusterStateAction.class);
    private static final String DEPRECATED_MESSAGE_MASTER_NODE =
        "Assigning [master_node] to parameter [metric] is deprecated and will be removed in 3.0. To support inclusive language, please use [cluster_manager_node] instead.";

    @Override
    public String getName() {
        return "cluster_state_action";
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_cluster/state"),
                new Route(GET, "/_cluster/state/{metric}"),
                new Route(GET, "/_cluster/state/{metric}/{indices}")
            )
        );
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ClusterStateRequest clusterStateRequest = Requests.clusterStateRequest();
        clusterStateRequest.indicesOptions(IndicesOptions.fromRequest(request, clusterStateRequest.indicesOptions()));
        clusterStateRequest.local(request.paramAsBoolean("local", clusterStateRequest.local()));
        clusterStateRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", clusterStateRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(clusterStateRequest, request, deprecationLogger, getName());
        if (request.hasParam("wait_for_metadata_version")) {
            clusterStateRequest.waitForMetadataVersion(request.paramAsLong("wait_for_metadata_version", 0));
        }
        clusterStateRequest.waitForTimeout(request.paramAsTime("wait_for_timeout", ClusterStateRequest.DEFAULT_WAIT_FOR_NODE_TIMEOUT));

        final String[] indices = Strings.splitStringByCommaToArray(request.param("indices", "_all"));
        boolean isAllIndicesOnly = indices.length == 1 && "_all".equals(indices[0]);
        if (!isAllIndicesOnly) {
            clusterStateRequest.indices(indices);
        }

        if (request.hasParam("metric")) {
            EnumSet<ClusterState.Metric> metrics = ClusterState.Metric.parseString(request.param("metric"), true);
            // do not ask for what we do not need.
            clusterStateRequest.nodes(
                metrics.contains(ClusterState.Metric.NODES)
                    || metrics.contains(ClusterState.Metric.MASTER_NODE)
                    || metrics.contains(ClusterState.Metric.CLUSTER_MANAGER_NODE)
            );
            // TODO: Remove the DeprecationLogger after removing MASTER_ROLE.
            // Because "_all" value will add all Metric into metrics set, for prevent deprecation message shown in that case,
            // add the check of validating metrics set doesn't contain all enum elements.
            if (!metrics.equals(EnumSet.allOf(ClusterState.Metric.class)) && metrics.contains(ClusterState.Metric.MASTER_NODE)) {
                deprecationLogger.deprecate("cluster_state_metric_parameter_master_node_value", DEPRECATED_MESSAGE_MASTER_NODE);
            }
            /*
             * there is no distinction in Java api between routing_table and routing_nodes, it's the same info set over the wire, one single
             * flag to ask for it
             */
            clusterStateRequest.routingTable(
                metrics.contains(ClusterState.Metric.ROUTING_TABLE) || metrics.contains(ClusterState.Metric.ROUTING_NODES)
            );
            clusterStateRequest.metadata(metrics.contains(ClusterState.Metric.METADATA));
            clusterStateRequest.blocks(metrics.contains(ClusterState.Metric.BLOCKS));
            clusterStateRequest.customs(metrics.contains(ClusterState.Metric.CUSTOMS));
        }
        settingsFilter.addFilterSettingParams(request);

        return channel -> client.admin().cluster().state(clusterStateRequest, new RestBuilderListener<ClusterStateResponse>(channel) {
            @Override
            public RestResponse buildResponse(ClusterStateResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                if (clusterStateRequest.waitForMetadataVersion() != null) {
                    builder.field(Fields.WAIT_FOR_TIMED_OUT, response.isWaitForTimedOut());
                }
                builder.field(Fields.CLUSTER_NAME, response.getClusterName().value());
                ToXContent.Params params = new ToXContent.DelegatingMapParams(
                    singletonMap(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API),
                    request
                );
                response.getState().toXContent(builder, params);
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>();
        responseParams.add("metric");
        responseParams.addAll(Settings.FORMAT_PARAMS);
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    static final class Fields {
        static final String WAIT_FOR_TIMED_OUT = "wait_for_timed_out";
        static final String CLUSTER_NAME = "cluster_name";
    }

}
