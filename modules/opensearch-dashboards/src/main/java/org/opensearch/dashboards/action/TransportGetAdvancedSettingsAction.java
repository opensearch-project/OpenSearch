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
import org.opensearch.OpenSearchStatusException;
import org.opensearch.Version;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.Map;

public class TransportGetAdvancedSettingsAction extends HandledTransportAction<GetAdvancedSettingsRequest, AdvancedSettingsResponse> {

    private static final Logger log = LogManager.getLogger(TransportGetAdvancedSettingsAction.class);

    private final Client client;
    private final String configKey;

    @Inject
    public TransportGetAdvancedSettingsAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(GetAdvancedSettingsAction.NAME, transportService, actionFilters, GetAdvancedSettingsRequest::new);
        this.client = client;
        this.configKey = "config:" + Version.CURRENT.toString();
    }

    @Override
    protected void doExecute(Task task, GetAdvancedSettingsRequest request, ActionListener<AdvancedSettingsResponse> listener) {
        try (final ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().stashContext()) {
            GetRequest getRequest = new GetRequest(request.getIndex(), configKey);

            client.get(getRequest, ActionListener.wrap(getResponse -> {
                ctx.restore();
                if (getResponse.isExists()) {
                    Map<String, Object> source = getResponse.getSourceAsMap();
                    listener.onResponse(new AdvancedSettingsResponse(source));
                } else {
                    listener.onFailure(new OpenSearchStatusException("Advanced settings not found", RestStatus.NOT_FOUND));
                }
            }, (e) -> {
                ctx.restore();
                if (e instanceof IndexNotFoundException) {
                    listener.onFailure(new OpenSearchStatusException("Advanced settings not found", RestStatus.NOT_FOUND));
                } else {
                    listener.onFailure(e);
                }
            }));
        }
    }
}
