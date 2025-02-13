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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.datastream.DataStreamsStatsAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Transport action to get data stream stats
 *
 * @opensearch.api
 */
public class RestDataStreamsStatsAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "data_stream_stats_action";
    }

    @Override
    public List<Route> routes() {
        return Arrays.asList(
            new Route(RestRequest.Method.GET, "/_data_stream/_stats"),
            new Route(RestRequest.Method.GET, "/_data_stream/{name}/_stats")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DataStreamsStatsAction.Request dataStreamsStatsRequest = new DataStreamsStatsAction.Request();
        boolean forbidClosedIndices = request.paramAsBoolean("forbid_closed_indices", true);
        IndicesOptions defaultIndicesOption = forbidClosedIndices
            ? dataStreamsStatsRequest.indicesOptions()
            : IndicesOptions.strictExpandOpen();
        assert dataStreamsStatsRequest.indicesOptions() == IndicesOptions.strictExpandOpenAndForbidClosed() : "DataStreamStats default "
            + "indices options changed";
        dataStreamsStatsRequest.indicesOptions(IndicesOptions.fromRequest(request, defaultIndicesOption));
        dataStreamsStatsRequest.indices(Strings.splitStringByCommaToArray(request.param("name")));
        return channel -> client.execute(DataStreamsStatsAction.INSTANCE, dataStreamsStatsRequest, new RestToXContentListener<>(channel));
    }
}
