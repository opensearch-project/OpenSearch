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

import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.client.Requests;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.core.ParseField;
import org.opensearch.common.Strings;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ObjectParser.ValueType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Transport action to reroute documents
 *
 * @opensearch.api
 */
public class RestClusterRerouteAction extends BaseRestHandler {
    private static final ObjectParser<ClusterRerouteRequest, Void> PARSER = new ObjectParser<>("cluster_reroute");
    static {
        PARSER.declareField(
            (p, v, c) -> v.commands(AllocationCommands.fromXContent(p)),
            new ParseField("commands"),
            ValueType.OBJECT_ARRAY
        );
        PARSER.declareBoolean(ClusterRerouteRequest::dryRun, new ParseField("dry_run"));
    }

    private static final String DEFAULT_METRICS = Strings.arrayToCommaDelimitedString(
        EnumSet.complementOf(EnumSet.of(ClusterState.Metric.METADATA)).toArray()
    );

    private final SettingsFilter settingsFilter;

    public RestClusterRerouteAction(SettingsFilter settingsFilter) {
        this.settingsFilter = settingsFilter;
    }

    // TODO: Remove the DeprecationLogger after removing MASTER_ROLE.
    // It's used to log deprecation when request parameter 'metric' contains 'master_node', or request parameter 'master_timeout' is used.
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestClusterRerouteAction.class);
    private static final String DEPRECATED_MESSAGE_MASTER_NODE =
        "Assigning [master_node] to parameter [metric] is deprecated and will be removed in 3.0. To support inclusive language, please use [cluster_manager_node] instead.";

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_cluster/reroute"));
    }

    @Override
    public String getName() {
        return "cluster_reroute_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        ClusterRerouteRequest clusterRerouteRequest = createRequest(request);
        settingsFilter.addFilterSettingParams(request);
        if (clusterRerouteRequest.explain()) {
            request.params().put("explain", Boolean.TRUE.toString());
        }
        // by default, return everything but metadata
        final String metric = request.param("metric");
        if (metric == null) {
            request.params().put("metric", DEFAULT_METRICS);
        } else {
            // TODO: Remove the statements in 'else' after removing MASTER_ROLE.
            EnumSet<ClusterState.Metric> metrics = ClusterState.Metric.parseString(request.param("metric"), true);
            // Because "_all" value will add all Metric into metrics set, for prevent deprecation message shown in that case,
            // add the check of validating metrics set doesn't contain all enum elements.
            if (!metrics.equals(EnumSet.allOf(ClusterState.Metric.class)) && metrics.contains(ClusterState.Metric.MASTER_NODE)) {
                deprecationLogger.deprecate("cluster_reroute_metric_parameter_master_node_value", DEPRECATED_MESSAGE_MASTER_NODE);
            }
        }
        return channel -> client.admin().cluster().reroute(clusterRerouteRequest, new RestToXContentListener<>(channel));
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

    public static ClusterRerouteRequest createRequest(RestRequest request) throws IOException {
        ClusterRerouteRequest clusterRerouteRequest = Requests.clusterRerouteRequest();
        clusterRerouteRequest.dryRun(request.paramAsBoolean("dry_run", clusterRerouteRequest.dryRun()));
        clusterRerouteRequest.explain(request.paramAsBoolean("explain", clusterRerouteRequest.explain()));
        clusterRerouteRequest.timeout(request.paramAsTime("timeout", clusterRerouteRequest.timeout()));
        clusterRerouteRequest.setRetryFailed(request.paramAsBoolean("retry_failed", clusterRerouteRequest.isRetryFailed()));
        clusterRerouteRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", clusterRerouteRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(clusterRerouteRequest, request, deprecationLogger, "cluster_reroute");
        request.applyContentParser(parser -> PARSER.parse(parser, clusterRerouteRequest, null));
        return clusterRerouteRequest;
    }
}
