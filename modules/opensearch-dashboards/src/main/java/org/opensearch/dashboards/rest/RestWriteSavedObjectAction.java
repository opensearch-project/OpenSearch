/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.rest;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dashboards.action.WriteSavedObjectAction;
import org.opensearch.dashboards.action.WriteSavedObjectRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * REST handler for PUT/POST /_opensearch_dashboards/saved_objects/{index}/{id}
 *
 * Query parameter "operation" controls create vs update (default: update).
 */
public class RestWriteSavedObjectAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "opensearch_dashboards_write_saved_object";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(PUT, "/_opensearch_dashboards/saved_objects/{index}/{id}"),
            new Route(POST, "/_opensearch_dashboards/saved_objects/{index}/{id}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        String documentId = request.param("id");
        String operation = request.param("operation", WriteSavedObjectRequest.OperationType.UPDATE.name());

        final Map<String, Object> document = new HashMap<>();
        if (request.hasContent()) {
            try (XContentParser parser = request.contentParser()) {
                document.putAll(parser.map());
            }
        }

        WriteSavedObjectRequest.OperationType operationType = WriteSavedObjectRequest.OperationType.valueOf(
            operation.toUpperCase(Locale.ROOT)
        );
        WriteSavedObjectRequest writeRequest = new WriteSavedObjectRequest(index, documentId, document, operationType);

        return channel -> client.execute(WriteSavedObjectAction.INSTANCE, writeRequest, new RestToXContentListener<>(channel));
    }
}
