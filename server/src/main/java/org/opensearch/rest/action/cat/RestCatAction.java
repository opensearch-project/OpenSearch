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

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Base _cat API endpoint
 *
 * @opensearch.api
 */
public class RestCatAction extends BaseRestHandler {

    private static final String CAT = "=^.^=";
    private static final String CAT_NL = CAT + "\n";
    private final String HELP;

    public RestCatAction(List<AbstractCatAction> catActions) {
        StringBuilder sb = new StringBuilder();
        sb.append(CAT_NL);
        for (AbstractCatAction catAction : catActions) {
            catAction.documentation(sb);
        }
        HELP = sb.toString();
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_cat"));
    }

    @Override
    public String getName() {
        return "cat_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, HELP));
    }

}
