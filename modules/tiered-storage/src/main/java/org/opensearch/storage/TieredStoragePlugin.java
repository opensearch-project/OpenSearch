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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.storage.prefetch.StoredFieldsPrefetch;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin to support writable warm index and other related features
 */
public class TieredStoragePlugin extends Plugin { // implements IndexStorePlugin, ActionPlugin, TelemetryAwarePlugin //

    /**
     * Default constructor.
     */
    public TieredStoragePlugin() {}

    /**
     * Index type for optimised downloads on hot indices.
     */
    // public static final String HOT_BLOCK_EAGER_FETCH_INDEX_TYPE = "hot_block_eager_fetch";
    // private static final String REMOTE_DOWNLOAD = "remote_download";
    // private final SetOnce<ThreadPool> threadpool = new SetOnce<>();
    // public static final String TIERED_COMPOSITE_INDEX_TYPE = "tiered-storage";
    private TieredStoragePrefetchSettings tieredStoragePrefetchSettings;
    // private TierActionMetrics tierActionMetrics;

    // private final List<Setting<?>> tieredStorageSettings = Stream.concat(
    // Stream.of(
    // // TieringUtils.H2W_MAX_CONCURRENT_TIEIRNG_REQUESTS,
    // // TieringUtils.W2H_MAX_CONCURRENT_TIEIRNG_REQUESTS,
    // // TieringUtils.JVM_USAGE_TIERING_THRESHOLD_PERCENT,
    // // TieringUtils.FILECACHE_ACTIVE_USAGE_TIERING_THRESHOLD_PERCENT,
    // TieredStoragePrefetchSettings.READ_AHEAD_BLOCK_COUNT,
    // TieredStoragePrefetchSettings.STORED_FIELDS_PREFETCH_ENABLED_SETTING
    // ),
    // // TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS.stream()
    // ).toList();

    // @Override
    // public Map<String, IndexStorePlugin.DirectoryFactory> getDirectoryFactories() {
    // return Collections.emptyMap();
    // }

    // @Override
    // public Map<String, IndexStorePlugin.CompositeDirectoryFactory> getCompositeDirectoryFactories() {
    // final Map<String, IndexStorePlugin.CompositeDirectoryFactory> registry = new HashMap<>();
    // registry.put(HOT_BLOCK_EAGER_FETCH_INDEX_TYPE, new OSBlockHotDirectoryFactory(() -> threadpool.get()));
    // registry.put(TIERED_COMPOSITE_INDEX_TYPE, new TieredDirectoryFactory(getPrefetchSettingsSupplier()));
    // return registry;
    // }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            TieredStoragePrefetchSettings.READ_AHEAD_BLOCK_COUNT,
            TieredStoragePrefetchSettings.STORED_FIELDS_PREFETCH_ENABLED_SETTING
        );
    }

    // public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
    // final int allocatedProcessors = OpenSearchExecutors.allocatedProcessors(settings);
    // ExecutorBuilder<?> executorBuilder = new ScalingExecutorBuilder(
    // REMOTE_DOWNLOAD,
    // 1,
    // ThreadPool.twiceAllocatedProcessors(allocatedProcessors),
    // TimeValue.timeValueMinutes(5)
    // );
    // return List.of(executorBuilder);
    // }

    // public ThreadPool getThreadpool() {
    // return threadpool.get();
    // }

    /**
     * Returns a supplier for the tiered storage prefetch settings.
     * @return supplier of {@link TieredStoragePrefetchSettings}
     */
    public Supplier<TieredStoragePrefetchSettings> getPrefetchSettingsSupplier() {
        return () -> this.tieredStoragePrefetchSettings;
    }

    // @Override
    // public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
    // if (FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)) {
    // return List.of(
    // new ActionHandler<>(HotToWarmTierAction.INSTANCE, TransportHotToWarmTierAction.class),
    // new ActionHandler<>(WarmToHotTierAction.INSTANCE, TransportWarmToHotTierAction.class),
    // new ActionHandler<>(CancelTieringAction.INSTANCE, TransportCancelTierAction.class),
    // new ActionHandler<>(ListTieringStatusAction.INSTANCE, TransportListTieringStatusAction.class),
    // new ActionHandler<>(GetTieringStatusAction.INSTANCE, TransportGetTieringStatusAction.class));
    // } else {
    // return List.of();
    // }
    // }

    // @Override
    // public List<RestHandler> getRestHandlers(
    // Settings settings, RestController restController,
    // ClusterSettings clusterSettings,
    // IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
    // IndexNameExpressionResolver indexNameExpressionResolver,
    // Supplier<DiscoveryNodes> nodesInCluster
    // ) {
    // if (FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)) {
    // return List.of(
    // new RestHotToWarmTierAction(),
    // new RestWarmToHotTierAction(),
    // new RestCancelTierAction(),
    // new RestGetTieringStatusAction(),
    // new RestListTieringStatusAction());
    // } else {
    // return List.of();
    // }
    // }

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
        // Tracer tracer,
        // MetricsRegistry metricsRegistry
    ) {
        // this.tierActionMetrics = new TierActionMetrics(metricsRegistry);
        this.tieredStoragePrefetchSettings = new TieredStoragePrefetchSettings(clusterService.getClusterSettings());
        // this.threadpool.set(threadPool);
        return Collections.emptyList();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)) {
            // indexModule.addSearchOperationListener(new TieredStorageSearchSlowLog(indexModule.getIndexSettings()));
            indexModule.addSearchOperationListener(new StoredFieldsPrefetch(getPrefetchSettingsSupplier()));
        }
    }

    // @Override
    // public Collection<Module> createGuiceModules() {
    // List<Module> modules = new ArrayList<>();
    // if (FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)) {
    // modules.add(new AbstractModule() {
    // @Override
    // protected void configure() {
    // bind(HotToWarmTieringService.class).asEagerSingleton();
    // bind(WarmToHotTieringService.class).asEagerSingleton();
    // bind(TierActionMetrics.class).toInstance(tierActionMetrics);
    // }
    // });
    // }
    // return Collections.unmodifiableCollection(modules);
    // }
}
