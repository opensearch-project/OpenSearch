/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class TransportWriteAdvancedSettingsAction extends HandledTransportAction<WriteAdvancedSettingsRequest, AdvancedSettingsResponse> {

    private static final Logger log = LogManager.getLogger(TransportWriteAdvancedSettingsAction.class);

    private final Client client;
    private final String configKey;

    @Inject
    public TransportWriteAdvancedSettingsAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(WriteAdvancedSettingsAction.NAME, transportService, actionFilters, WriteAdvancedSettingsRequest::new);
        this.client = client;
        this.configKey = "config:" + Version.CURRENT.toString();
    }

    @Override
    protected void doExecute(Task task, WriteAdvancedSettingsRequest request, ActionListener<AdvancedSettingsResponse> listener) {
        if (request.isCreateOperation()) {
            handleCreateOperation(request, listener);
        } else {
            handleUpdateOperation(request, listener);
        }
    }

    private void handleCreateOperation(WriteAdvancedSettingsRequest request, ActionListener<AdvancedSettingsResponse> listener) {
        Map<String, Object> doc = Map.of(
            "type",
            "config",
            "config",
            request.getSettings(),
            "references",
            List.of(),
            "updated_at",
            DateFormatter.forPattern("strict_date_time").format(Instant.now())
        );

        IndexRequest indexRequest = new IndexRequest(request.getIndex()).id(configKey).source(doc);
        client.index(
            indexRequest,
            ActionListener.wrap(indexResponse -> { listener.onResponse(new AdvancedSettingsResponse(doc)); }, listener::onFailure)
        );
    }

    private void handleUpdateOperation(WriteAdvancedSettingsRequest request, ActionListener<AdvancedSettingsResponse> listener) {
        Map<String, Object> doc = Map.of(
            "type",
            "config",
            "config",
            request.getSettings(),
            "references",
            List.of(),
            "updated_at",
            DateFormatter.forPattern("strict_date_time").format(Instant.now())
        );

        IndexRequest indexRequest = new IndexRequest(request.getIndex()).id(configKey).source(doc);
        client.index(
            indexRequest,
            ActionListener.wrap(indexResponse -> listener.onResponse(new AdvancedSettingsResponse(doc)), listener::onFailure)
        );
    }
}
