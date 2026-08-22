/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.rest;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dashboards.action.WriteAdvancedSettingsAction;
import org.opensearch.dashboards.action.WriteAdvancedSettingsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.PUT;

public class RestWriteAdvancedSettingsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "opensearch_dashboards_write_advanced_settings";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_opensearch_dashboards/advanced_settings/{index}/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        String documentId = request.param("id");
        String operation = request.param("operation", WriteAdvancedSettingsRequest.OperationType.UPDATE.name());

        final Map<String, Object> document = new HashMap<>();
        if (request.hasContent()) {
            try (XContentParser parser = request.contentParser()) {
                document.putAll(parser.map());
            }
        }

        WriteAdvancedSettingsRequest.OperationType operationType = WriteAdvancedSettingsRequest.OperationType.valueOf(
            operation.toUpperCase(Locale.ROOT)
        );
        WriteAdvancedSettingsRequest writeRequest = new WriteAdvancedSettingsRequest(index, documentId, document, operationType);

        return channel -> client.execute(WriteAdvancedSettingsAction.INSTANCE, writeRequest, new RestToXContentListener<>(channel));
    }
}
