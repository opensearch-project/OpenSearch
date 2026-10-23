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
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;

/** Resolves definitions using the setting registry. Extends the read action so any node can serve
 * the request (offloading the elected cluster-manager); the response is static settings-registry
 * metadata, so the possible stale read of the Read action does not affect correctness.
 * @opensearch.internal
 */
public class TransportClusterDescribeSettingsAction extends TransportClusterManagerNodeReadAction<
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
            resolver,
            true
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
