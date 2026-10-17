/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;

/** Resolves definitions using the elected cluster-manager's setting registry.
 * @opensearch.internal
 */
public class TransportClusterDescribeSettingsAction extends TransportClusterManagerNodeAction<
    ClusterDescribeSettingsRequest,
    ClusterDescribeSettingsResponse> {
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportClusterDescribeSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver resolver,
        SettingsFilter settingsFilter
    ) {
        super(
            ClusterDescribeSettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterDescribeSettingsRequest::new,
            resolver
        );
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected ClusterDescribeSettingsResponse read(StreamInput in) throws IOException {
        return new ClusterDescribeSettingsResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterDescribeSettingsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void clusterManagerOperation(
        ClusterDescribeSettingsRequest request,
        ClusterState state,
        ActionListener<ClusterDescribeSettingsResponse> listener
    ) {
        listener.onResponse(describe(request, clusterService.getClusterSettings(), settingsFilter));
    }

    static ClusterDescribeSettingsResponse describe(
        ClusterDescribeSettingsRequest request,
        ClusterSettings registry,
        SettingsFilter filter
    ) {
        // Filter names using placeholders, never read or serialize actual/default values.
        Settings.Builder names = Settings.builder();
        for (String name : request.names())
            names.put(name, "");
        var result = new HashMap<String, Boolean>();
        for (String name : filter.filter(names.build()).keySet()) {
            Setting<?> setting = registry.get(name);
            // Omission deliberately makes an unknown name indistinguishable from a filtered one.
            if (setting != null && setting.isFiltered() == false) result.put(name, setting.isDynamic());
        }
        return new ClusterDescribeSettingsResponse(result);
    }
}
