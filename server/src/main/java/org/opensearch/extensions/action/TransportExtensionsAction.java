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
import org.opensearch.node.Node;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportExtensionsAction extends HandledTransportAction<ExtensionsActionRequest, ExtensionsActionResponse> {

    private final String nodeName;
    private final ClusterService clusterService;

    @Inject
    public TransportExtensionsAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(ExtensionsAction.NAME, transportService, actionFilters, ExtensionsActionRequest::new);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, ExtensionsActionRequest request, ActionListener<ExtensionsActionResponse> listener) {
        listener.onResponse(new ExtensionsActionResponse("HelloWorld"));
    }
}
