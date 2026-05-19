/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Rest action for scaling index operations
 *
 * @opensearch.internal
 */
public class RestScaleIndexAction extends BaseRestHandler {

    private static final String SEARCH_ONLY_FIELD = "search_only";

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/{index}/_scale")));
    }

    @Override
    public String getName() {
        return "search_only_index_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        if (index == null || index.trim().isEmpty()) {
            throw new IllegalArgumentException("index is required");
        }

        // Parse the request body first to get the scale down value
        final boolean searchOnly = parseSearchOnlyValue(request);

        // Then use the final value in the lambda
        return channel -> client.admin().indices().prepareScaleSearchOnly(index, searchOnly).execute(new RestToXContentListener<>(channel));
    }

    /**
     * Parses and validates the search_only parameter from the request body.
     */
    private boolean parseSearchOnlyValue(RestRequest request) {
        try {
            Map<String, Object> source;
            try {
                source = request.contentParser().map();
            } catch (Exception e) {
                throw new IllegalArgumentException("Request body must be valid JSON", e);
            }
            for (String key : source.keySet()) {
                if (SEARCH_ONLY_FIELD.equals(key) == false) {
                    throw new IllegalArgumentException("Unknown parameter [" + key + "]. Only [" + SEARCH_ONLY_FIELD + "] is allowed.");
                }
            }
            if (source.containsKey(SEARCH_ONLY_FIELD) == false) {
                throw new IllegalArgumentException("Parameter [" + SEARCH_ONLY_FIELD + "] is required");
            }
            Object value = source.get(SEARCH_ONLY_FIELD);
            if ((value instanceof Boolean) == false) {
                throw new IllegalArgumentException("Parameter [" + SEARCH_ONLY_FIELD + "] must be a boolean (true or false)");
            }
            return (Boolean) value;
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                throw e;
            }
            throw new IllegalArgumentException("Request body must be valid JSON", e);
        }
    }
}
