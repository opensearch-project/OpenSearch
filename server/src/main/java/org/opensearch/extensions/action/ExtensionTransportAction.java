/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.extensions.ExtensionsOrchestrator;
import org.opensearch.node.Node;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * The main proxy transport action used to proxy a transport request from extension to another extension
 *
 * @opensearch.internal
 */
public class ExtensionTransportAction extends HandledTransportAction<ExtensionActionRequest, ExtensionActionResponse> {

    private final String nodeName;
    private final ClusterService clusterService;
    private final ExtensionsOrchestrator extensionsOrchestrator;

    @Inject
    public ExtensionTransportAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ExtensionsOrchestrator extensionsOrchestrator
    ) {
        super(ExtensionMainAction.NAME, transportService, actionFilters, ExtensionActionRequest::new);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.clusterService = clusterService;
        this.extensionsOrchestrator = extensionsOrchestrator;
    }

    @Override
    protected void doExecute(Task task, ExtensionActionRequest request, ActionListener<ExtensionActionResponse> listener) {
        try {
            listener.onResponse(extensionsOrchestrator.handleTransportRequest(request));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
