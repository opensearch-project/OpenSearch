/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Rest action for creating PIT context
 */
public class RestCreatePitAction extends BaseRestHandler {
    public static String ALLOW_PARTIAL_PIT_CREATION = "allow_partial_pit_creation";
    public static String KEEP_ALIVE = "keep_alive";

    @Override
    public String getName() {
        return "create_pit_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        boolean allowPartialPitCreation = request.paramAsBoolean(ALLOW_PARTIAL_PIT_CREATION, true);
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        TimeValue keepAlive = request.paramAsTime(KEEP_ALIVE, null);
        CreatePitRequest createPitRequest = new CreatePitRequest(keepAlive, allowPartialPitCreation, indices);
        createPitRequest.setIndicesOptions(IndicesOptions.fromRequest(request, createPitRequest.indicesOptions()));
        createPitRequest.setPreference(request.param("preference"));
        createPitRequest.setRouting(request.param("routing"));

        return channel -> client.createPit(createPitRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/{index}/_search/point_in_time")));
    }

}
