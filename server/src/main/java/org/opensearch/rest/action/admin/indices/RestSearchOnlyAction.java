/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.core.xcontent.XContentParser;
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
public class RestSearchOnlyAction extends BaseRestHandler {

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
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        if (index == null || index.trim().isEmpty()) {
            throw new IllegalArgumentException("index is required");
        }

        Map<String, Object> sourceAsMap = null;
        if (request.method() == POST) {
            try (XContentParser parser = request.contentParser()) {
                sourceAsMap = parser.map();
            }
            if (sourceAsMap != null) {
                for (String key : sourceAsMap.keySet()) {
                    if (!SEARCH_ONLY_FIELD.equals(key)) {
                        throw new IllegalArgumentException("Unknown parameter [" + key + "]. Only [" + SEARCH_ONLY_FIELD + "] is allowed.");
                    }
                }
            }
        }

        final boolean searchOnly;
        if (sourceAsMap != null) {
            searchOnly = sourceAsMap.containsKey(SEARCH_ONLY_FIELD) && (boolean) sourceAsMap.get(SEARCH_ONLY_FIELD);
        } else {
            searchOnly = false;
        }

        return channel -> client.admin()
            .indices()
            .prepareSearchOnly(index)
            .setScaleDown(searchOnly)
            .execute(new RestToXContentListener<>(channel));
    }
}
