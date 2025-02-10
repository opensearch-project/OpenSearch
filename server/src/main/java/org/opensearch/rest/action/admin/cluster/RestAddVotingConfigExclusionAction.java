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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Transport action to add voting config exclusions
 *
 * @opensearch.api
 */
public class RestAddVotingConfigExclusionAction extends BaseRestHandler {
    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30L);
    private static final Logger logger = LogManager.getLogger(RestAddVotingConfigExclusionAction.class);

    private static final String DEPRECATION_MESSAGE = "POST /_cluster/voting_config_exclusions/{node_name} "
        + "will be removed in a future version. "
        + "Please use POST /_cluster/voting_config_exclusions?node_ids=... "
        + "or POST /_cluster/voting_config_exclusions?node_names=... instead.";

    @Override
    public String getName() {
        return "add_voting_config_exclusions_action";
    }

    @Override
    public List<Route> routes() {
        return Arrays.asList(
            new DeprecatedRoute(POST, "/_cluster/voting_config_exclusions/{node_name}", DEPRECATION_MESSAGE),
            new Route(POST, "/_cluster/voting_config_exclusions")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        AddVotingConfigExclusionsRequest votingConfigExclusionsRequest = resolveVotingConfigExclusionsRequest(request);
        return channel -> client.execute(
            AddVotingConfigExclusionsAction.INSTANCE,
            votingConfigExclusionsRequest,
            new RestToXContentListener<>(channel)
        );
    }

    AddVotingConfigExclusionsRequest resolveVotingConfigExclusionsRequest(final RestRequest request) {
        String deprecatedNodeDescription = null;
        String nodeIds = null;
        String nodeNames = null;

        if (request.hasParam("node_name")) {
            deprecatedNodeDescription = request.param("node_name");
        }

        if (request.hasParam("node_ids")) {
            nodeIds = request.param("node_ids");
        }

        if (request.hasParam("node_names")) {
            nodeNames = request.param("node_names");
        }

        return new AddVotingConfigExclusionsRequest(
            Strings.splitStringByCommaToArray(deprecatedNodeDescription),
            Strings.splitStringByCommaToArray(nodeIds),
            Strings.splitStringByCommaToArray(nodeNames),
            TimeValue.parseTimeValue(request.param("timeout"), DEFAULT_TIMEOUT, getClass().getSimpleName() + ".timeout")
        );
    }

}
