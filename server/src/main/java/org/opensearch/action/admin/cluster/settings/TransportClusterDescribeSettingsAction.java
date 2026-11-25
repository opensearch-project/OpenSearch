/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.SettingsDescriber;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/** Resolves definitions using the setting registry. The registry is identical on every node of the same version and
 * never depends on cluster state, so the request is answered locally by whichever node receives it: no cluster-manager
 * round-trip, no term check, and the response never crosses the wire. For the same reason, cluster blocks are
 * deliberately not consulted: the answer is valid while the cluster state is still recovering or no cluster-manager is
 * elected, which is when an operator most needs it. Authorization is unaffected; it is applied by {@code ActionFilter}s
 * before {@link #doExecute} runs, which is why the transport action exists at all.
 * @opensearch.internal
 */
public class TransportClusterDescribeSettingsAction extends HandledTransportAction<
    ClusterDescribeSettingsRequest,
    ClusterDescribeSettingsResponse> {
    private final ClusterService clusterService;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportClusterDescribeSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        SettingsFilter settingsFilter
    ) {
        super(ClusterDescribeSettingsAction.NAME, transportService, actionFilters, ClusterDescribeSettingsRequest::new);
        this.clusterService = clusterService;
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected void doExecute(Task task, ClusterDescribeSettingsRequest request, ActionListener<ClusterDescribeSettingsResponse> listener) {
        listener.onResponse(
            new ClusterDescribeSettingsResponse(
                SettingsDescriber.describe(request.names(), clusterService.getClusterSettings(), settingsFilter)
            )
        );
    }
}
