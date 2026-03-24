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
 * Plugin to support writable warm index and other related features.
 */
public class TieredStoragePlugin extends Plugin {

    /**
     * Default constructor.
     */
    public TieredStoragePlugin() {}

    private TieredStoragePrefetchSettings tieredStoragePrefetchSettings;

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            TieredStoragePrefetchSettings.READ_AHEAD_BLOCK_COUNT,
            TieredStoragePrefetchSettings.STORED_FIELDS_PREFETCH_ENABLED_SETTING
        );
    }

    /**
     * Returns a supplier for the tiered storage prefetch settings.
     * @return supplier of {@link TieredStoragePrefetchSettings}
     */
    public Supplier<TieredStoragePrefetchSettings> getPrefetchSettingsSupplier() {
        return () -> this.tieredStoragePrefetchSettings;
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
