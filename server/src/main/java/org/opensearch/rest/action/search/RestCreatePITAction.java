/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.search.CreatePITRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.rest.RestRequest.Method.POST;

public class RestCreatePITAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "create_pit_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        boolean allowPartialPitCreation = request.paramAsBoolean("allow_partial_pit_creation", true);
        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        TimeValue keepAlive = request.paramAsTime("keep_alive", null);
        ActionRequestValidationException validationException = null;
        if (keepAlive == null) {
            validationException = addValidationError("Keep alive cannot be empty", validationException);
        }
        ExceptionsHelper.reThrowIfNotNull(validationException);
        CreatePITRequest createPitRequest = new CreatePITRequest(keepAlive, allowPartialPitCreation, indices);
        createPitRequest.setIndicesOptions(IndicesOptions.fromRequest(request, createPitRequest.indicesOptions()));
        createPitRequest.setPreference(request.param("preference"));
        createPitRequest.setRouting(request.param("routing"));

        return channel -> client.createPit(createPitRequest, new RestStatusToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/{index}/_pit")));
    }

}
