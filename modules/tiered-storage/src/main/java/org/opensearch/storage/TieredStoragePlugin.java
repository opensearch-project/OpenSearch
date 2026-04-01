/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryAwarePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.storage.common.tiering.TieringUtils;
import org.opensearch.storage.metrics.TierActionMetrics;
import org.opensearch.storage.prefetch.StoredFieldsPrefetch;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.opensearch.storage.slowlogs.TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS;

/**
 * Plugin to support writable warm index and other related features.
 * Registers settings, actions, REST handlers, directory factories, and search operation listeners.
 *
 * getDirectoryFactories, getCompositeDirectoryFactories, getSettings, getExecutorBuilders,
 * getActions, getRestHandlers, createComponents, onIndexModule, and createGuiceModules
 * will be added in the implementation PR.
 */
public class TieredStoragePlugin extends Plugin implements IndexStorePlugin, ActionPlugin, TelemetryAwarePlugin {

    /**
     * Index type for optimised downloads on hot indices.
     */
    public static final String HOT_BLOCK_EAGER_FETCH_INDEX_TYPE = "hot_block_eager_fetch";
    /** Composite index type for tiered storage. */
    public static final String TIERED_COMPOSITE_INDEX_TYPE = "tiered-storage";
    private static final String REMOTE_DOWNLOAD = "remote_download";

    private final SetOnce<ThreadPool> threadpool = new SetOnce<>();
    private TieredStoragePrefetchSettings tieredStoragePrefetchSettings;
    private TierActionMetrics tierActionMetrics;

    /** Constructs a new TieredStoragePlugin. */
    public TieredStoragePlugin() {}

    private final List<Setting<?>> tieredStorageSettings = Stream.concat(
        Stream.of(
            TieringUtils.H2W_MAX_CONCURRENT_TIEIRNG_REQUESTS,
            TieringUtils.W2H_MAX_CONCURRENT_TIEIRNG_REQUESTS,
            TieringUtils.JVM_USAGE_TIERING_THRESHOLD_PERCENT,
            TieringUtils.FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT,
            TieredStoragePrefetchSettings.READ_AHEAD_BLOCK_COUNT,
            TieredStoragePrefetchSettings.STORED_FIELDS_PREFETCH_ENABLED_SETTING
        ),
        TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS.stream()
    ).toList();

    /**
     * Returns a supplier for the tiered storage prefetch settings.
     * @return supplier of {@link TieredStoragePrefetchSettings}
     */
    public Supplier<TieredStoragePrefetchSettings> getPrefetchSettingsSupplier() {
        return () -> this.tieredStoragePrefetchSettings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return tieredStorageSettings;
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.tieredStoragePrefetchSettings = new TieredStoragePrefetchSettings(clusterService.getClusterSettings());
        return Collections.emptyList();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)) {
            indexModule.addSearchOperationListener(new StoredFieldsPrefetch(getPrefetchSettingsSupplier()));
        }
    }
}
