/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.analytics.settings.AnalyticsQuerySettings;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Pins the freshness contract of {@link ShardTargetResolver#resolve}: each call must re-resolve
 * the index name against the supplied {@link ClusterState}, never a snapshot taken at
 * construction time. A regression that caches concrete indices upfront would dispatch to stale
 * shards when an alias gets re-pointed mid-query.
 */
public class ShardTargetResolverTests extends OpenSearchTestCase {

    private RelOptCluster cluster;
    private JavaTypeFactoryImpl typeFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), rexBuilder);
    }

    /**
     * Two cluster states with the same alias pointing to different concrete indices. The first
     * resolve() must produce shards on node-a; the second must produce shards on node-b. If the
     * resolver cached state at construction, both calls would return the same targets.
     */
    public void testResolveReRunsAgainstPassedClusterState() {
        // Two distinct mocked cluster states. The alias expression is the same; the resolver is
        // configured to map it to a different concrete index per cluster-state object identity.
        ClusterState stateA = mock(ClusterState.class);
        ClusterState stateB = mock(ClusterState.class);
        Metadata metaA = mock(Metadata.class);
        Metadata metaB = mock(Metadata.class);
        when(stateA.metadata()).thenReturn(metaA);
        when(stateB.metadata()).thenReturn(metaB);
        // Empty getIndicesLookup() forces IndexResolution.resolve to fall through to the resolver
        // path (no literal alias match), which is the path under test.
        when(metaA.getIndicesLookup()).thenReturn(new TreeMap<>());
        when(metaB.getIndicesLookup()).thenReturn(new TreeMap<>());
        // After the resolver returns concrete names, IndexResolution.resolve looks each up via
        // metadata().index(...). Return a mapping-less IndexMetadata so schema-compat validation
        // no-ops (it skips indices whose mapping() returns null).
        IndexMetadata imdA = mock(IndexMetadata.class);
        IndexMetadata imdB = mock(IndexMetadata.class);
        when(imdA.getIndex()).thenReturn(new Index("idx_a", "uuid-a"));
        when(imdB.getIndex()).thenReturn(new Index("idx_b", "uuid-b"));
        when(metaA.index("idx_a")).thenReturn(imdA);
        when(metaB.index("idx_b")).thenReturn(imdB);
        DiscoveryNodes nodesA = mock(DiscoveryNodes.class);
        DiscoveryNodes nodesB = mock(DiscoveryNodes.class);
        when(stateA.nodes()).thenReturn(nodesA);
        when(stateB.nodes()).thenReturn(nodesB);

        DiscoveryNode nodeA = mock(DiscoveryNode.class);
        DiscoveryNode nodeB = mock(DiscoveryNode.class);
        when(nodeA.getId()).thenReturn("node-a");
        when(nodeB.getId()).thenReturn("node-b");
        when(nodesA.get("node-a")).thenReturn(nodeA);
        when(nodesB.get("node-b")).thenReturn(nodeB);

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(
            resolver.concreteIndexNames(
                eq(stateA),
                any(IndicesOptions.class),
                org.mockito.ArgumentMatchers.anyBoolean(),
                any(String[].class)
            )
        ).thenReturn(new String[] { "idx_a" });
        when(
            resolver.concreteIndexNames(
                eq(stateB),
                any(IndicesOptions.class),
                org.mockito.ArgumentMatchers.anyBoolean(),
                any(String[].class)
            )
        ).thenReturn(new String[] { "idx_b" });

        ShardId shardA = new ShardId(new Index("idx_a", "uuid-a"), 0);
        ShardId shardB = new ShardId(new Index("idx_b", "uuid-b"), 0);
        ShardRouting routingA = mock(ShardRouting.class);
        ShardRouting routingB = mock(ShardRouting.class);
        when(routingA.currentNodeId()).thenReturn("node-a");
        when(routingB.currentNodeId()).thenReturn("node-b");
        when(routingA.shardId()).thenReturn(shardA);
        when(routingB.shardId()).thenReturn(shardB);
        ShardIterator iterA = mock(ShardIterator.class);
        ShardIterator iterB = mock(ShardIterator.class);
        when(iterA.nextOrNull()).thenReturn(routingA);
        when(iterB.nextOrNull()).thenReturn(routingB);

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting routing = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(routing);
        when(routing.searchShards(eq(stateA), eq(new String[] { "idx_a" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(List.of(iterA))
        );
        when(routing.searchShards(eq(stateB), eq(new String[] { "idx_b" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(List.of(iterB))
        );

        RelNode fragment = stubScanForAlias("my_alias");
        ShardTargetResolver resolverUnderTest = new ShardTargetResolver(fragment, clusterService, resolver);

        // First resolve hits state A → shard on node-a / idx_a.
        List<ExecutionTarget> targetsA = resolverUnderTest.resolve(stateA, null);
        assertEquals(1, targetsA.size());
        assertSame("first resolve must surface state-A's node", nodeA, targetsA.get(0).node());
        assertEquals("first resolve must surface state-A's shard", shardA, ((ShardExecutionTarget) targetsA.get(0)).shardId());

        // Second resolve hits state B → shard on node-b / idx_b. If the resolver cached state,
        // this assertion fails (targets would still be on node-a / idx_a).
        List<ExecutionTarget> targetsB = resolverUnderTest.resolve(stateB, null);
        assertEquals(1, targetsB.size());
        assertSame("second resolve must surface state-B's node", nodeB, targetsB.get(0).node());
        assertEquals("second resolve must surface state-B's shard", shardB, ((ShardExecutionTarget) targetsB.get(0)).shardId());
    }

    /**
     * When an alias resolves to multiple indices and the total shard count exceeds the limit,
     * resolve() must throw an {@link IllegalArgumentException} with the alias name in the message.
     */
    public void testResolveRejectsAliasExceedingMaxShardsPerQuery() {
        int limit = 3;

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);

        // Set up alias in the indices lookup so IndexResolution takes the alias path.
        IndexMetadata imdA = mock(IndexMetadata.class);
        IndexMetadata imdB = mock(IndexMetadata.class);
        when(imdA.getIndex()).thenReturn(new Index("idx_a", "uuid-a"));
        when(imdB.getIndex()).thenReturn(new Index("idx_b", "uuid-b"));
        when(imdA.getState()).thenReturn(IndexMetadata.State.OPEN);
        when(imdB.getState()).thenReturn(IndexMetadata.State.OPEN);
        AliasMetadata aliasMd = mock(AliasMetadata.class);
        when(aliasMd.filteringRequired()).thenReturn(false);
        when(imdA.getAliases()).thenReturn(Map.of("my_alias", aliasMd));
        when(imdB.getAliases()).thenReturn(Map.of("my_alias", aliasMd));

        IndexAbstraction aliasAbstraction = mock(IndexAbstraction.class);
        when(aliasAbstraction.getType()).thenReturn(IndexAbstraction.Type.ALIAS);
        when(aliasAbstraction.getIndices()).thenReturn(List.of(imdA, imdB));
        TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
        lookup.put("my_alias", aliasAbstraction);
        when(metadata.getIndicesLookup()).thenReturn(lookup);

        when(metadata.index("idx_a")).thenReturn(imdA);
        when(metadata.index("idx_b")).thenReturn(imdB);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);

        // 5 total shards across the two indices.
        int shardCount = 5;
        List<ShardIterator> iterators = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node-" + i);
            when(nodes.get("node-" + i)).thenReturn(node);
            ShardRouting routing = mock(ShardRouting.class);
            when(routing.currentNodeId()).thenReturn("node-" + i);
            String idx = i < 3 ? "idx_a" : "idx_b";
            String uuid = i < 3 ? "uuid-a" : "uuid-b";
            when(routing.shardId()).thenReturn(new ShardId(new Index(idx, uuid), i % 3));
            ShardIterator iter = mock(ShardIterator.class);
            when(iter.nextOrNull()).thenReturn(routing);
            iterators.add(iter);
        }

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(
            resolver.concreteIndexNames(
                eq(clusterState),
                any(IndicesOptions.class),
                org.mockito.ArgumentMatchers.anyBoolean(),
                any(String[].class)
            )
        ).thenReturn(new String[] { "idx_a", "idx_b" });

        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.getKey(), limit).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting opRouting = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(opRouting);
        when(opRouting.searchShards(eq(clusterState), eq(new String[] { "idx_a", "idx_b" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(iterators)
        );

        RelNode fragment = stubScanForAlias("my_alias");
        ShardTargetResolver resolverUnderTest = new ShardTargetResolver(fragment, clusterService, resolver);
        resolverUnderTest.setMaxShardsPerQuery(limit);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> resolverUnderTest.resolve(clusterState, null));
        assertTrue(ex.getMessage().contains("alias [my_alias]"));
        assertTrue(ex.getMessage().contains("[" + shardCount + "] shards"));
        assertTrue(ex.getMessage().contains("[" + limit + "]"));
        assertTrue(ex.getMessage().contains("analytics.query.max_shards_per_query"));
    }

    /**
     * A single concrete index with many shards must NOT be rejected even if shard count
     * exceeds the limit — the limit only applies to multi-index queries.
     */
    public void testResolveAllowsSingleIndexExceedingLimit() {
        int shardCount = 5;
        int limit = 3;

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndicesLookup()).thenReturn(new TreeMap<>());
        IndexMetadata imd = mock(IndexMetadata.class);
        when(imd.getIndex()).thenReturn(new Index("big_index", "uuid-big"));
        when(metadata.index("big_index")).thenReturn(imd);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);

        List<ShardIterator> iterators = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node-" + i);
            when(nodes.get("node-" + i)).thenReturn(node);
            ShardRouting routing = mock(ShardRouting.class);
            when(routing.currentNodeId()).thenReturn("node-" + i);
            when(routing.shardId()).thenReturn(new ShardId(new Index("big_index", "uuid-big"), i));
            ShardIterator iter = mock(ShardIterator.class);
            when(iter.nextOrNull()).thenReturn(routing);
            iterators.add(iter);
        }

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(
            resolver.concreteIndexNames(
                eq(clusterState),
                any(IndicesOptions.class),
                org.mockito.ArgumentMatchers.anyBoolean(),
                any(String[].class)
            )
        ).thenReturn(new String[] { "big_index" });

        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.getKey(), limit).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting opRouting = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(opRouting);
        when(opRouting.searchShards(eq(clusterState), eq(new String[] { "big_index" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(iterators)
        );

        RelNode fragment = stubScanForAlias("big_index");
        ShardTargetResolver resolverUnderTest = new ShardTargetResolver(fragment, clusterService, resolver);
        resolverUnderTest.setMaxShardsPerQuery(limit);

        List<ExecutionTarget> targets = resolverUnderTest.resolve(clusterState, null);
        assertEquals(shardCount, targets.size());
    }

    /**
     * When the resolved shard count is exactly at the limit for a multi-index query,
     * resolve() must succeed.
     */
    public void testResolveSucceedsAtExactLimitForAlias() {
        int limit = 3;

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);

        IndexMetadata imdA = mock(IndexMetadata.class);
        IndexMetadata imdB = mock(IndexMetadata.class);
        when(imdA.getIndex()).thenReturn(new Index("idx_a", "uuid-a"));
        when(imdB.getIndex()).thenReturn(new Index("idx_b", "uuid-b"));
        when(imdA.getState()).thenReturn(IndexMetadata.State.OPEN);
        when(imdB.getState()).thenReturn(IndexMetadata.State.OPEN);
        AliasMetadata aliasMd = mock(AliasMetadata.class);
        when(aliasMd.filteringRequired()).thenReturn(false);
        when(imdA.getAliases()).thenReturn(Map.of("my_alias", aliasMd));
        when(imdB.getAliases()).thenReturn(Map.of("my_alias", aliasMd));

        IndexAbstraction aliasAbstraction = mock(IndexAbstraction.class);
        when(aliasAbstraction.getType()).thenReturn(IndexAbstraction.Type.ALIAS);
        when(aliasAbstraction.getIndices()).thenReturn(List.of(imdA, imdB));
        TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
        lookup.put("my_alias", aliasAbstraction);
        when(metadata.getIndicesLookup()).thenReturn(lookup);

        when(metadata.index("idx_a")).thenReturn(imdA);
        when(metadata.index("idx_b")).thenReturn(imdB);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);

        // Exactly 3 shards across 2 indices — at the limit.
        List<ShardIterator> iterators = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node-" + i);
            when(nodes.get("node-" + i)).thenReturn(node);
            ShardRouting routing = mock(ShardRouting.class);
            when(routing.currentNodeId()).thenReturn("node-" + i);
            String idx = i < 2 ? "idx_a" : "idx_b";
            String uuid = i < 2 ? "uuid-a" : "uuid-b";
            when(routing.shardId()).thenReturn(new ShardId(new Index(idx, uuid), i % 2));
            ShardIterator iter = mock(ShardIterator.class);
            when(iter.nextOrNull()).thenReturn(routing);
            iterators.add(iter);
        }

        IndexNameExpressionResolver resolver = mock(IndexNameExpressionResolver.class);
        when(
            resolver.concreteIndexNames(
                eq(clusterState),
                any(IndicesOptions.class),
                org.mockito.ArgumentMatchers.anyBoolean(),
                any(String[].class)
            )
        ).thenReturn(new String[] { "idx_a", "idx_b" });

        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.getKey(), limit).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting opRouting = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(opRouting);
        when(opRouting.searchShards(eq(clusterState), eq(new String[] { "idx_a", "idx_b" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(iterators)
        );

        RelNode fragment = stubScanForAlias("my_alias");
        ShardTargetResolver resolverUnderTest = new ShardTargetResolver(fragment, clusterService, resolver);
        resolverUnderTest.setMaxShardsPerQuery(limit);

        List<ExecutionTarget> targets = resolverUnderTest.resolve(clusterState, null);
        assertEquals(limit, targets.size());
    }

    /** Minimal table scan referencing {@code aliasName} so {@code findTableName} surfaces it. */
    private TableScan stubScanForAlias(String aliasName) {
        RelDataType rowType = typeFactory.builder().add("v", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of(aliasName));
        when(table.getRowType()).thenReturn(rowType);
        return new TableScan(cluster, cluster.traitSet(), List.of(), table) {
            @Override
            public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
                return this;
            }
        };
    }
}
