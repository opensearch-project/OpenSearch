/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.main;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ProtobufActionFilters;
import org.opensearch.action.support.ProtobufHandledTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.Node;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.transport.TransportService;

/**
 * Performs the main action
*
* @opensearch.internal
*/
public class ProtobufTransportMainAction extends ProtobufHandledTransportAction<ProtobufMainRequest, ProtobufMainResponse> {

    private final String nodeName;
    private final ClusterService clusterService;

    @Inject
    public ProtobufTransportMainAction(
        Settings settings,
        TransportService transportService,
        ProtobufActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(MainAction.NAME, transportService, actionFilters, ProtobufMainRequest::new);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(ProtobufTask task, ProtobufMainRequest request, ActionListener<ProtobufMainResponse> listener) {
        ClusterState clusterState = clusterService.state();
        ClusterName clusterName = new ClusterName(clusterState.getClusterName().value());
        listener.onResponse(
            new ProtobufMainResponse(nodeName, Version.CURRENT, clusterName, clusterState.metadata().clusterUUID(), Build.CURRENT)
        );
    }
}
