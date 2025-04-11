/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster;

import org.opensearch.cluster.action.index.MappingUpdatedAction;
import org.opensearch.cluster.action.index.NodeMappingRefreshAction;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.cluster.metadata.ComponentTemplateMetadata;
import org.opensearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.opensearch.cluster.metadata.DataStreamMetadata;
import org.opensearch.cluster.metadata.IndexGraveyard;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataDeleteIndexService;
import org.opensearch.cluster.metadata.MetadataIndexAliasesService;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.metadata.MetadataIndexTemplateService;
import org.opensearch.cluster.metadata.MetadataMappingService;
import org.opensearch.cluster.metadata.MetadataUpdateSettingsService;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.ViewMetadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.metadata.WorkloadGroupMetadata;
import org.opensearch.cluster.routing.DelayedAllocationService;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ConcurrentRecoveriesAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.NodeLoadAwareAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.RebalanceOnlyWhenActiveAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.RemoteStoreMigrationAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ResizeAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.RestoreInProgressAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.SearchReplicaAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.SnapshotInProgressAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.TargetPoolAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry.Entry;
import org.opensearch.core.common.io.stream.Writeable.Reader;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.GatewayAllocator;
import org.opensearch.gateway.ShardsBatchGatewayAllocator;
import org.opensearch.ingest.IngestMetadata;
import org.opensearch.persistent.PersistentTasksCustomMetadata;
import org.opensearch.persistent.PersistentTasksNodeService;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.script.ScriptMetadata;
import org.opensearch.search.pipeline.SearchPipelineMetadata;
import org.opensearch.snapshots.SnapshotsInfoService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResultsService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Configures classes and services that affect the entire cluster.
 *
 * @opensearch.internal
 */
public class ClusterModule extends AbstractModule {

    public static final String BALANCED_ALLOCATOR = "balanced"; // default
    public static final Setting<String> SHARDS_ALLOCATOR_TYPE_SETTING = new Setting<>(
        "cluster.routing.allocation.type",
        BALANCED_ALLOCATOR,
        Function.identity(),
        Property.NodeScope
    );

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final AllocationDeciders allocationDeciders;
    private final AllocationService allocationService;
    private final List<ClusterPlugin> clusterPlugins;
    // pkg private for tests
    final Collection<AllocationDecider> deciderList;
    final ShardsAllocator shardsAllocator;
    private final ClusterManagerMetrics clusterManagerMetrics;

    public ClusterModule(
        Settings settings,
        ClusterService clusterService,
        List<ClusterPlugin> clusterPlugins,
        ClusterInfoService clusterInfoService,
        SnapshotsInfoService snapshotsInfoService,
        ThreadContext threadContext,
        ClusterManagerMetrics clusterManagerMetrics
    ) {
        this.clusterPlugins = clusterPlugins;
        this.deciderList = createAllocationDeciders(settings, clusterService.getClusterSettings(), clusterPlugins);
        this.allocationDeciders = new AllocationDeciders(deciderList);
        this.shardsAllocator = createShardsAllocator(settings, clusterService.getClusterSettings(), clusterPlugins);
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver(threadContext);
        this.allocationService = new AllocationService(
            allocationDeciders,
            shardsAllocator,
            clusterInfoService,
            snapshotsInfoService,
            settings,
            clusterManagerMetrics
        );
        this.clusterManagerMetrics = clusterManagerMetrics;
    }

    public static List<Entry> getNamedWriteables() {
        List<Entry> entries = new ArrayList<>();
        // Cluster State
        registerClusterCustom(entries, SnapshotsInProgress.TYPE, SnapshotsInProgress::new, SnapshotsInProgress::readDiffFrom);
        registerClusterCustom(entries, RestoreInProgress.TYPE, RestoreInProgress::new, RestoreInProgress::readDiffFrom);
        registerClusterCustom(
            entries,
            SnapshotDeletionsInProgress.TYPE,
            SnapshotDeletionsInProgress::new,
            SnapshotDeletionsInProgress::readDiffFrom
        );
        registerClusterCustom(
            entries,
            RepositoryCleanupInProgress.TYPE,
            RepositoryCleanupInProgress::new,
            RepositoryCleanupInProgress::readDiffFrom
        );
        // Metadata
        registerMetadataCustom(entries, RepositoriesMetadata.TYPE, RepositoriesMetadata::new, RepositoriesMetadata::readDiffFrom);
        registerMetadataCustom(entries, IngestMetadata.TYPE, IngestMetadata::new, IngestMetadata::readDiffFrom);
        registerMetadataCustom(entries, SearchPipelineMetadata.TYPE, SearchPipelineMetadata::new, SearchPipelineMetadata::readDiffFrom);
        registerMetadataCustom(entries, ScriptMetadata.TYPE, ScriptMetadata::new, ScriptMetadata::readDiffFrom);
        registerMetadataCustom(entries, IndexGraveyard.TYPE, IndexGraveyard::new, IndexGraveyard::readDiffFrom);
        registerMetadataCustom(
            entries,
            PersistentTasksCustomMetadata.TYPE,
            PersistentTasksCustomMetadata::new,
            PersistentTasksCustomMetadata::readDiffFrom
        );
        registerMetadataCustom(
            entries,
            ComponentTemplateMetadata.TYPE,
            ComponentTemplateMetadata::new,
            ComponentTemplateMetadata::readDiffFrom
        );
        registerMetadataCustom(
            entries,
            ComposableIndexTemplateMetadata.TYPE,
            ComposableIndexTemplateMetadata::new,
            ComposableIndexTemplateMetadata::readDiffFrom
        );
        registerMetadataCustom(entries, DataStreamMetadata.TYPE, DataStreamMetadata::new, DataStreamMetadata::readDiffFrom);
        registerMetadataCustom(entries, ViewMetadata.TYPE, ViewMetadata::new, ViewMetadata::readDiffFrom);
        registerMetadataCustom(entries, WeightedRoutingMetadata.TYPE, WeightedRoutingMetadata::new, WeightedRoutingMetadata::readDiffFrom);
        registerMetadataCustom(
            entries,
            DecommissionAttributeMetadata.TYPE,
            DecommissionAttributeMetadata::new,
            DecommissionAttributeMetadata::readDiffFrom
        );

        registerMetadataCustom(entries, WorkloadGroupMetadata.TYPE, WorkloadGroupMetadata::new, WorkloadGroupMetadata::readDiffFrom);
        // Task Status (not Diffable)
        entries.add(new Entry(Task.Status.class, PersistentTasksNodeService.Status.NAME, PersistentTasksNodeService.Status::new));
        return entries;
    }

    static final Set<String> PRE_6_3_METADATA_CUSTOMS_WHITE_LIST = Collections.unmodifiableSet(
        Sets.newHashSet(IndexGraveyard.TYPE, IngestMetadata.TYPE, RepositoriesMetadata.TYPE, ScriptMetadata.TYPE)
    );

    static final Set<String> PRE_6_3_CLUSTER_CUSTOMS_WHITE_LIST = Collections.unmodifiableSet(
        Sets.newHashSet(RestoreInProgress.TYPE, SnapshotDeletionsInProgress.TYPE, SnapshotsInProgress.TYPE)
    );

    /**
     * For interoperability with transport clients older than 6.3, we need to strip customs
     * from the cluster state that the client might not be able to deserialize
     *
     * @param clusterState the cluster state to filter the customs from
     * @return the adapted cluster state
     */
    public static ClusterState filterCustomsForPre63Clients(ClusterState clusterState) {
        final ClusterState.Builder builder = ClusterState.builder(clusterState);
        clusterState.customs().keySet().iterator().forEachRemaining(name -> {
            if (PRE_6_3_CLUSTER_CUSTOMS_WHITE_LIST.contains(name) == false) {
                builder.removeCustom(name);
            }
        });
        final Metadata.Builder metaBuilder = Metadata.builder(clusterState.metadata());
        clusterState.metadata().customs().keySet().iterator().forEachRemaining(name -> {
            if (PRE_6_3_METADATA_CUSTOMS_WHITE_LIST.contains(name) == false) {
                metaBuilder.removeCustom(name);
            }
        });
        return builder.metadata(metaBuilder).build();
    }

    public static List<NamedXContentRegistry.Entry> getNamedXWriteables() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        // Metadata
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(RepositoriesMetadata.TYPE),
                RepositoriesMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IngestMetadata.TYPE), IngestMetadata::fromXContent)
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(SearchPipelineMetadata.TYPE),
                SearchPipelineMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(ScriptMetadata.TYPE), ScriptMetadata::fromXContent)
        );
        entries.add(
            new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IndexGraveyard.TYPE), IndexGraveyard::fromXContent)
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(PersistentTasksCustomMetadata.TYPE),
                PersistentTasksCustomMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(ComponentTemplateMetadata.TYPE),
                ComponentTemplateMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(ComposableIndexTemplateMetadata.TYPE),
                ComposableIndexTemplateMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(DataStreamMetadata.TYPE),
                DataStreamMetadata::fromXContent
            )
        );
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(ViewMetadata.TYPE), ViewMetadata::fromXContent));
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(WeightedRoutingMetadata.TYPE),
                WeightedRoutingMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(DecommissionAttributeMetadata.TYPE),
                DecommissionAttributeMetadata::fromXContent
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new ParseField(WorkloadGroupMetadata.TYPE),
                WorkloadGroupMetadata::fromXContent
            )
        );
        return entries;
    }

    private static <T extends ClusterState.Custom> void registerClusterCustom(
        List<Entry> entries,
        String name,
        Reader<? extends T> reader,
        Reader<NamedDiff> diffReader
    ) {
        registerCustom(entries, ClusterState.Custom.class, name, reader, diffReader);
    }

    private static <T extends Metadata.Custom> void registerMetadataCustom(
        List<Entry> entries,
        String name,
        Reader<? extends T> reader,
        Reader<NamedDiff> diffReader
    ) {
        registerCustom(entries, Metadata.Custom.class, name, reader, diffReader);
    }

    private static <T extends NamedWriteable> void registerCustom(
        List<Entry> entries,
        Class<T> category,
        String name,
        Reader<? extends T> reader,
        Reader<NamedDiff> diffReader
    ) {
        entries.add(new Entry(category, name, reader));
        entries.add(new Entry(NamedDiff.class, name, diffReader));
    }

    public IndexNameExpressionResolver getIndexNameExpressionResolver() {
        return indexNameExpressionResolver;
    }

    // TODO: this is public so allocation benchmark can access the default deciders...can we do that in another way?
    /** Return a new {@link AllocationDecider} instance with builtin deciders as well as those from plugins. */
    public static Collection<AllocationDecider> createAllocationDeciders(
        Settings settings,
        ClusterSettings clusterSettings,
        List<ClusterPlugin> clusterPlugins
    ) {
        // collect deciders by class so that we can detect duplicates
        Map<Class, AllocationDecider> deciders = new LinkedHashMap<>();
        addAllocationDecider(deciders, new MaxRetryAllocationDecider());
        addAllocationDecider(deciders, new ResizeAllocationDecider());
        addAllocationDecider(deciders, new ReplicaAfterPrimaryActiveAllocationDecider());
        addAllocationDecider(deciders, new RebalanceOnlyWhenActiveAllocationDecider());
        addAllocationDecider(deciders, new ClusterRebalanceAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ConcurrentRebalanceAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ConcurrentRecoveriesAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new EnableAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new NodeVersionAllocationDecider(settings));
        addAllocationDecider(deciders, new SnapshotInProgressAllocationDecider());
        addAllocationDecider(deciders, new RestoreInProgressAllocationDecider());
        addAllocationDecider(deciders, new FilterAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new SearchReplicaAllocationDecider());
        addAllocationDecider(deciders, new SameShardAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new DiskThresholdDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ThrottlingAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new ShardsLimitAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new AwarenessAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new NodeLoadAwareAllocationDecider(settings, clusterSettings));
        addAllocationDecider(deciders, new TargetPoolAllocationDecider());
        addAllocationDecider(deciders, new RemoteStoreMigrationAllocationDecider(settings, clusterSettings));

        clusterPlugins.stream()
            .flatMap(p -> p.createAllocationDeciders(settings, clusterSettings).stream())
            .forEach(d -> addAllocationDecider(deciders, d));

        return deciders.values();
    }

    /** Add the given allocation decider to the given deciders collection, erroring if the class name is already used. */
    private static void addAllocationDecider(Map<Class, AllocationDecider> deciders, AllocationDecider decider) {
        if (deciders.put(decider.getClass(), decider) != null) {
            throw new IllegalArgumentException("Cannot specify allocation decider [" + decider.getClass().getName() + "] twice");
        }
    }

    private static ShardsAllocator createShardsAllocator(
        Settings settings,
        ClusterSettings clusterSettings,
        List<ClusterPlugin> clusterPlugins
    ) {
        Map<String, Supplier<ShardsAllocator>> allocators = new HashMap<>();
        allocators.put(BALANCED_ALLOCATOR, () -> new BalancedShardsAllocator(settings, clusterSettings));

        for (ClusterPlugin plugin : clusterPlugins) {
            plugin.getShardsAllocators(settings, clusterSettings).forEach((k, v) -> {
                if (allocators.put(k, v) != null) {
                    throw new IllegalArgumentException("ShardsAllocator [" + k + "] already defined");
                }
            });
        }
        String allocatorName = SHARDS_ALLOCATOR_TYPE_SETTING.get(settings);
        Supplier<ShardsAllocator> allocatorSupplier = allocators.get(allocatorName);
        if (allocatorSupplier == null) {
            throw new IllegalArgumentException("Unknown ShardsAllocator [" + allocatorName + "]");
        }
        return Objects.requireNonNull(allocatorSupplier.get(), "ShardsAllocator factory for [" + allocatorName + "] returned null");
    }

    public AllocationService getAllocationService() {
        return allocationService;
    }

    @Override
    protected void configure() {
        bind(GatewayAllocator.class).asEagerSingleton();
        bind(ShardsBatchGatewayAllocator.class).asEagerSingleton();
        bind(AllocationService.class).toInstance(allocationService);
        bind(ClusterService.class).toInstance(clusterService);
        bind(NodeConnectionsService.class).asEagerSingleton();
        bind(MetadataDeleteIndexService.class).asEagerSingleton();
        bind(MetadataIndexStateService.class).asEagerSingleton();
        bind(MetadataMappingService.class).asEagerSingleton();
        bind(MetadataIndexAliasesService.class).asEagerSingleton();
        bind(MetadataUpdateSettingsService.class).asEagerSingleton();
        bind(MetadataIndexTemplateService.class).asEagerSingleton();
        bind(IndexNameExpressionResolver.class).toInstance(indexNameExpressionResolver);
        bind(DelayedAllocationService.class).asEagerSingleton();
        bind(ShardStateAction.class).asEagerSingleton();
        bind(NodeMappingRefreshAction.class).asEagerSingleton();
        bind(MappingUpdatedAction.class).asEagerSingleton();
        bind(TaskResultsService.class).asEagerSingleton();
        bind(AllocationDeciders.class).toInstance(allocationDeciders);
        bind(ShardsAllocator.class).toInstance(shardsAllocator);
        bind(ClusterManagerMetrics.class).toInstance(clusterManagerMetrics);
    }

    public void setExistingShardsAllocators(GatewayAllocator gatewayAllocator, ShardsBatchGatewayAllocator shardsBatchGatewayAllocator) {
        final Map<String, ExistingShardsAllocator> existingShardsAllocators = new HashMap<>();
        existingShardsAllocators.put(GatewayAllocator.ALLOCATOR_NAME, gatewayAllocator);
        existingShardsAllocators.put(ShardsBatchGatewayAllocator.ALLOCATOR_NAME, shardsBatchGatewayAllocator);
        for (ClusterPlugin clusterPlugin : clusterPlugins) {
            for (Map.Entry<String, ExistingShardsAllocator> existingShardsAllocatorEntry : clusterPlugin.getExistingShardsAllocators()
                .entrySet()) {
                final String allocatorName = existingShardsAllocatorEntry.getKey();
                if (existingShardsAllocators.put(allocatorName, existingShardsAllocatorEntry.getValue()) != null) {
                    throw new IllegalArgumentException(
                        "ExistingShardsAllocator ["
                            + allocatorName
                            + "] from ["
                            + clusterPlugin.getClass().getName()
                            + "] was already defined"
                    );
                }
            }
        }
        allocationService.setExistingShardsAllocators(existingShardsAllocators);
    }

    public void setRerouteServiceForAllocator(RerouteService rerouteService) {
        shardsAllocator.setRerouteService(rerouteService);
    }
}
