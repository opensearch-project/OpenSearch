/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.ValidationException;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.indices.IndexCreationException;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Sandbox plugin that provides a {@link CompositeIndexingExecutionEngine} for
 * orchestrating multi-format indexing. Discovers {@link DataFormatPlugin}
 * instances during node bootstrap via the {@link ExtensiblePlugin} SPI and
 * creates a composite engine when composite indexing is enabled for an index.
 *
 * <p>Registers two index settings:
 * <ul>
 *   <li>{@code index.composite.primary_data_format} — designates the primary
 *       format (default {@code "lucene"})</li>
 *   <li>{@code index.composite.secondary_data_formats} — lists the secondary
 *       formats (default empty)</li>
 * </ul>
 *
 * <p>And three cluster settings:
 * <ul>
 *   <li>{@code cluster.composite.primary_data_format} — cluster-level default for the primary format</li>
 *   <li>{@code cluster.composite.secondary_data_formats} — cluster-level default for secondary formats</li>
 *   <li>{@code cluster.restrict.composite.dataformat} — when true, rejects index-level overrides that
 *       differ from the cluster defaults</li>
 * </ul>
 *
 * <p>Format plugins (e.g., Parquet) extend this plugin by declaring
 * {@code extendedPlugins = ['composite-engine']} in their {@code build.gradle}
 * and implementing {@link DataFormatPlugin}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeDataFormatPlugin extends Plugin implements DataFormatPlugin {

    private static final Logger logger = LogManager.getLogger(CompositeDataFormatPlugin.class);

    /**
     * Populated during {@link #createComponents} so the {@link IndexSettingProvider} registered by
     * {@link #getAdditionalIndexSettingProviders()} can read live cluster-scope default settings
     * at index-creation time.
     */
    private ClusterService clusterService;

    /**
     * Index setting that designates the primary data format for an index.
     * The primary format is the authoritative format used for merge operations.
     */
    public static final Setting<String> PRIMARY_DATA_FORMAT = Setting.simpleString(
        "index.composite.primary_data_format",
        "lucene",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Index setting that lists the secondary data formats for an index.
     * Secondary formats receive writes alongside the primary but are not used
     * as the merge authority.
     */
    public static final Setting<List<String>> SECONDARY_DATA_FORMATS = Setting.listSetting(
        "index.composite.secondary_data_formats",
        Collections.emptyList(),
        s -> s,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Cluster-level default for {@code index.composite.primary_data_format}.
     * When the index setting is not explicitly provided, this cluster setting is used as the fallback.
     */
    public static final Setting<String> CLUSTER_PRIMARY_DATA_FORMAT = Setting.simpleString(
        "cluster.composite.primary_data_format",
        "lucene",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cluster-level default for {@code index.composite.secondary_data_formats}.
     * When the index setting is not explicitly provided, this cluster setting is used as the fallback.
     */
    public static final Setting<List<String>> CLUSTER_SECONDARY_DATA_FORMATS = Setting.listSetting(
        "cluster.composite.secondary_data_formats",
        Collections.emptyList(),
        s -> s,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * If enabled, this cluster setting enforces that indexes will be created with composite data-format settings
     * matching the cluster-level defaults defined in {@link #CLUSTER_PRIMARY_DATA_FORMAT} and
     * {@link #CLUSTER_SECONDARY_DATA_FORMATS} by rejecting any request that specifies an index-level value
     * that does not match. If disabled, users may choose the composite data-format on a per-index basis using the
     * {@link #PRIMARY_DATA_FORMAT} and {@link #SECONDARY_DATA_FORMATS} settings.
     *
     * <p>This is scoped to the composite plugin so restriction can be toggled independently of the server-level
     * {@code cluster.restrict.pluggable.dataformat} flag that governs the core
     * {@code index.pluggable.dataformat.*} settings.
     */
    public static final Setting<Boolean> CLUSTER_RESTRICT_COMPOSITE_DATAFORMAT_SETTING = Setting.boolSetting(
        "cluster.restrict.composite.dataformat",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
    public CompositeDataFormatPlugin() {}

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            PRIMARY_DATA_FORMAT,
            SECONDARY_DATA_FORMATS,
            CLUSTER_PRIMARY_DATA_FORMAT,
            CLUSTER_SECONDARY_DATA_FORMATS,
            CLUSTER_RESTRICT_COMPOSITE_DATAFORMAT_SETTING
        );
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
        this.clusterService = clusterService;
        return Collections.emptyList();
    }

    /**
     * Stamps the cluster-scope defaults for {@link #PRIMARY_DATA_FORMAT} and
     * {@link #SECONDARY_DATA_FORMATS} into newly created indices when those index-level settings
     * are not supplied by the request or a matching template.
     *
     * <p>Because both index settings are {@link Setting.Property#Final}, the effective value is
     * resolved once at index-creation time from the live {@link ClusterSettings} registry and
     * frozen into the index metadata. Later updates to the {@code cluster.composite.*} settings
     * affect only indices created after the update.
     *
     * <p>If {@link #createComponents} has not run yet (e.g. during early bootstrap), the provider
     * contributes no settings so that index creation falls back to the per-setting defaults.
     */
    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders() {
        return Collections.singletonList(new IndexSettingProvider() {
            @Override
            public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
                if (clusterService == null) {
                    return Settings.EMPTY;
                }
                ClusterSettings clusterSettings = clusterService.getClusterSettings();

                List<String> skiplist = clusterSettings.get(IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_RESTRICT_SKIPLIST);
                if (skiplist.stream().anyMatch(indexName::startsWith)) {
                    return Settings.EMPTY;
                }

                boolean restrict = clusterSettings.get(CLUSTER_RESTRICT_COMPOSITE_DATAFORMAT_SETTING);
                String clusterPrimary = clusterSettings.get(CLUSTER_PRIMARY_DATA_FORMAT);
                List<String> clusterSecondary = clusterSettings.get(CLUSTER_SECONDARY_DATA_FORMATS);

                if (restrict) {
                    List<String> errors = new ArrayList<>();
                    if (PRIMARY_DATA_FORMAT.exists(templateAndRequestSettings)
                        && PRIMARY_DATA_FORMAT.get(templateAndRequestSettings).equals(clusterPrimary) == false) {
                        errors.add(
                            "index setting ["
                                + PRIMARY_DATA_FORMAT.getKey()
                                + "] cannot differ from cluster default ["
                                + clusterPrimary
                                + "] when ["
                                + CLUSTER_RESTRICT_COMPOSITE_DATAFORMAT_SETTING.getKey()
                                + "=true]"
                        );
                    }
                    if (SECONDARY_DATA_FORMATS.exists(templateAndRequestSettings)
                        && SECONDARY_DATA_FORMATS.get(templateAndRequestSettings).equals(clusterSecondary) == false) {
                        errors.add(
                            "index setting ["
                                + SECONDARY_DATA_FORMATS.getKey()
                                + "] cannot differ from cluster default "
                                + clusterSecondary
                                + " when ["
                                + CLUSTER_RESTRICT_COMPOSITE_DATAFORMAT_SETTING.getKey()
                                + "=true]"
                        );
                    }
                    if (errors.isEmpty() == false) {
                        ValidationException validationException = new ValidationException();
                        validationException.addValidationErrors(errors);
                        throw new IndexCreationException(indexName, validationException);
                    }
                }

                Settings.Builder out = Settings.builder();
                if (PRIMARY_DATA_FORMAT.exists(templateAndRequestSettings) == false) {
                    out.put(PRIMARY_DATA_FORMAT.getKey(), clusterPrimary);
                }
                if (SECONDARY_DATA_FORMATS.exists(templateAndRequestSettings) == false) {
                    out.putList(SECONDARY_DATA_FORMATS.getKey(), clusterSecondary);
                }
                return out.build();
            }
        });
    }

    @Override
    public DataFormat getDataFormat() {
        return new CompositeDataFormat();
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings) {
        return new CompositeIndexingExecutionEngine(
            settings.indexSettings(),
            settings.mapperService(),
            settings.committer(),
            settings.registry(),
            settings.store(),
            settings.checksumStrategies()
        );
    }

    @Override
    public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(
        IndexSettings indexSettings,
        DataFormatRegistry dataFormatRegistry
    ) {
        Settings settings = indexSettings.getSettings();
        String primaryFormatName = PRIMARY_DATA_FORMAT.get(settings);
        List<String> secondaryFormatNames = SECONDARY_DATA_FORMATS.get(settings);

        Map<String, Supplier<DataFormatDescriptor>> descriptors = new HashMap<>();
        if (primaryFormatName != null) {
            descriptors.putAll(dataFormatRegistry.getFormatDescriptors(indexSettings, dataFormatRegistry.format(primaryFormatName)));
        }
        for (String secondaryName : secondaryFormatNames) {
            if (secondaryName != null) {
                descriptors.putAll(dataFormatRegistry.getFormatDescriptors(indexSettings, dataFormatRegistry.format(secondaryName)));
            }
        }
        return Map.copyOf(descriptors);
    }

    /**
     * Returns the store strategies from every participating sub-format plugin
     * (primary + secondary), keyed by format name. Mirrors {@link #getFormatDescriptors}:
     * each participating format is resolved through the registry, which delegates
     * to the sub-plugin without re-entering this composite.
     */
    @Override
    public Map<DataFormat, StoreStrategy> getStoreStrategies(IndexSettings indexSettings, DataFormatRegistry dataFormatRegistry) {
        Settings settings = indexSettings.getSettings();
        String primaryFormatName = PRIMARY_DATA_FORMAT.get(settings);
        List<String> secondaryFormatNames = SECONDARY_DATA_FORMATS.get(settings);

        Map<DataFormat, StoreStrategy> strategies = new HashMap<>();
        if (primaryFormatName != null && primaryFormatName.isEmpty() == false) {
            strategies.putAll(dataFormatRegistry.getStoreStrategies(indexSettings, dataFormatRegistry.format(primaryFormatName)));
        }
        for (String secondaryName : secondaryFormatNames) {
            if (secondaryName != null && secondaryName.isEmpty() == false) {
                strategies.putAll(dataFormatRegistry.getStoreStrategies(indexSettings, dataFormatRegistry.format(secondaryName)));
            }
        }
        return Map.copyOf(strategies);
    }
}
