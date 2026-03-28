/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.rest;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dashboards.action.GetAdvancedSettingsAction;
import org.opensearch.dashboards.action.GetAdvancedSettingsRequest;
import org.opensearch.dashboards.action.WriteAdvancedSettingsAction;
import org.opensearch.dashboards.action.WriteAdvancedSettingsRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.PUT;

public class RestWriteAdvancedSettingsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "opensearch_dashboards_write_advanced_settings";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_opensearch_dashboards/advanced_settings/{index}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");

        final Map<String, Object> newSettings = new HashMap<>();
        if (request.hasContent()) {
            try (XContentParser parser = request.contentParser()) {
                newSettings.putAll(parser.map());
            }
        }

        return channel -> {
            GetAdvancedSettingsRequest getRequest = new GetAdvancedSettingsRequest(index);

            client.execute(GetAdvancedSettingsAction.INSTANCE, getRequest, ActionListener.wrap(getResponse -> {
                Map<String, Object> updatedSettings = new HashMap<>();

                Object config = getResponse.getSettings().get("config");
                if (config instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> configMap = (Map<String, Object>) config;
                    updatedSettings.putAll(configMap);
                }
                updatedSettings.putAll(newSettings);

                // Document exists, use UPDATE operation
                WriteAdvancedSettingsRequest writeRequest = new WriteAdvancedSettingsRequest(
                    index,
                    updatedSettings,
                    WriteAdvancedSettingsRequest.OperationType.UPDATE
                );
                client.execute(WriteAdvancedSettingsAction.INSTANCE, writeRequest, new RestToXContentListener<>(channel));
            }, e -> {
                // Document doesn't exist, use CREATE operation
                WriteAdvancedSettingsRequest writeRequest = new WriteAdvancedSettingsRequest(
                    index,
                    newSettings,
                    WriteAdvancedSettingsRequest.OperationType.CREATE
                );
                client.execute(WriteAdvancedSettingsAction.INSTANCE, writeRequest, new RestToXContentListener<>(channel));
            }));
        };
    }
}
