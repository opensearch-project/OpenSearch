/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingMetadata;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.HashMap;

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
        listener.onResponse(describe(request, clusterService.getClusterSettings(), settingsFilter));
    }

    static ClusterDescribeSettingsResponse describe(
        ClusterDescribeSettingsRequest request,
        ClusterSettings registry,
        SettingsFilter filter
    ) {
        // Filter names using placeholders; the configured value is never read or serialized.
        Settings.Builder names = Settings.builder();
        for (String name : request.names())
            names.put(name, "");
        var result = new HashMap<String, SettingMetadata>();
        for (String name : filter.filter(names.build()).keySet()) {
            Setting<?> setting = registry.get(name);
            // Omission deliberately makes an unknown name indistinguishable from a filtered one.
            if (setting != null && setting.isFiltered() == false) {
                result.put(name, metadataOf(setting));
            }
        }
        return new ClusterDescribeSettingsResponse(result);
    }

    /**
     * Describes a single registered setting. The declared default is resolved against {@link Settings#EMPTY} so that
     * settings whose default depends on another setting report their declared default rather than a value derived
     * from this cluster's configuration.
     */
    private static SettingMetadata metadataOf(Setting<?> setting) {
        boolean dynamic = setting.isDynamic();
        // SecureSetting overrides getDefaultRaw to throw UnsupportedOperationException; never call it.
        if (setting instanceof SecureSetting) {
            return SettingMetadata.withoutDefault(dynamic);
        }
        String defaultValue = setting.getDefaultRaw(Settings.EMPTY);
        // The Setting constructor asserts a non-null default for every non-secure setting, so only emptiness is
        // checked here. Group settings declare an empty default; an empty string carries no information for any setting.
        if (defaultValue.isEmpty()) {
            return SettingMetadata.withoutDefault(dynamic);
        }
        return SettingMetadata.withDefault(dynamic, defaultValue);
    }
}
