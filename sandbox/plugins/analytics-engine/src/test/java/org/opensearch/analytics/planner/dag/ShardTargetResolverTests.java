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
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.analytics.planner.IndexResolution;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexMetadata;
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
 * Pins the freshness contract of {@link ShardTargetResolver#resolve}: each call must use the
 * supplied {@link ClusterState} for shard routing, even though the carried {@link IndexResolution}
 * (set at planning time) determines which concrete indices are targeted. A regression that
 * caches shard routing upfront would dispatch to stale shards when the routing table changes.
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
     * Two cluster states with the same concrete indices routed to different nodes. The first
     * resolve() must produce shards on node-a; the second must produce shards on node-b. The
     * carried resolution fixes the concrete indices but shard routing is re-evaluated against
     * the cluster state passed to each resolve() call.
     */
    public void testResolveReRunsAgainstPassedClusterState() {
        // The carried resolution is fixed — same concrete index for both calls.
        IndexResolution resolution = stubResolution("my_alias", "idx_a");

        ClusterState stateA = mock(ClusterState.class);
        ClusterState stateB = mock(ClusterState.class);
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

        ShardId shardA = new ShardId(new Index("idx_a", "uuid-a"), 0);
        ShardId shardB = new ShardId(new Index("idx_a", "uuid-a"), 0);
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
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting routing = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(routing);
        when(routing.searchShards(eq(stateA), eq(new String[] { "idx_a" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(List.of(iterA))
        );
        when(routing.searchShards(eq(stateB), eq(new String[] { "idx_a" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(List.of(iterB))
        );

        OpenSearchTableScan scan = createScanWithResolution("my_alias", resolution);
        ShardTargetResolver resolverUnderTest = new ShardTargetResolver(scan, clusterService);

        // First resolve hits state A → shard on node-a.
        List<ExecutionTarget> targetsA = resolverUnderTest.resolve(stateA, null);
        assertEquals(1, targetsA.size());
        assertSame("first resolve must surface state-A's node", nodeA, targetsA.get(0).node());
        assertEquals("first resolve must surface state-A's shard", shardA, ((ShardExecutionTarget) targetsA.get(0)).shardId());

        // Second resolve hits state B → shard on node-b. If the resolver cached routing state,
        // this assertion fails (targets would still be on node-a).
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

        // Build a carried resolution for an alias with 2 backing indices.
        IndexMetadata imdA = mockIndexMetadata("idx_a", "uuid-a");
        IndexMetadata imdB = mockIndexMetadata("idx_b", "uuid-b");
        IndexResolution resolution = stubResolution("my_alias", List.of(imdA, imdB));

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);

        // describeIndexSource looks up the alias in the indices lookup
        IndexAbstraction aliasAbstraction = mock(IndexAbstraction.class);
        when(aliasAbstraction.getType()).thenReturn(IndexAbstraction.Type.ALIAS);
        TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
        lookup.put("my_alias", aliasAbstraction);
        when(metadata.getIndicesLookup()).thenReturn(lookup);

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);

        // 5 total shards across the two indices.
        int shardCount = 5;
        List<ShardIterator> iterators = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node-" + i);
            when(nodes.get("node-" + i)).thenReturn(node);
            ShardRouting shardRouting = mock(ShardRouting.class);
            when(shardRouting.currentNodeId()).thenReturn("node-" + i);
            String idx = i < 3 ? "idx_a" : "idx_b";
            String uuid = i < 3 ? "uuid-a" : "uuid-b";
            when(shardRouting.shardId()).thenReturn(new ShardId(new Index(idx, uuid), i % 3));
            ShardIterator iter = mock(ShardIterator.class);
            when(iter.nextOrNull()).thenReturn(shardRouting);
            iterators.add(iter);
        }

        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey(), limit).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting opRouting = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(opRouting);
        when(opRouting.searchShards(eq(clusterState), eq(new String[] { "idx_a", "idx_b" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(iterators)
        );

        OpenSearchTableScan scan = createScanWithResolution("my_alias", resolution);
        ShardTargetResolver resolverUnderTest = new ShardTargetResolver(scan, clusterService);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> resolverUnderTest.resolve(clusterState, null));
        assertTrue(ex.getMessage().contains("alias [my_alias]"));
        assertTrue(ex.getMessage().contains("[" + shardCount + "] shards"));
        assertTrue(ex.getMessage().contains("[" + limit + "]"));
        assertTrue(ex.getMessage().contains("action.search.shard_count.limit"));
    }

    /**
     * The ceiling counts shards, not indices: one oversharded concrete index is exactly as expensive
     * for the coordinator as the same shards spread across an alias, so it is rejected too. Vanilla's
     * {@code failIfOverShardCountLimit} draws no distinction either, and it can afford not to because
     * the limit is unlimited by default — see {@link #testResolveIsUnlimitedByDefault}.
     */
    public void testResolveRejectsSingleIndexExceedingLimit() {
        int shardCount = 5;
        int limit = 3;

        // Carried resolution with a single concrete index.
        IndexMetadata imd = mockIndexMetadata("big_index", "uuid-big");
        IndexResolution resolution = stubResolution("big_index", List.of(imd));

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);

        // describeIndexSource looks up the index name in the indices lookup
        IndexAbstraction indexAbstraction = mock(IndexAbstraction.class);
        when(indexAbstraction.getType()).thenReturn(IndexAbstraction.Type.CONCRETE_INDEX);
        TreeMap<String, IndexAbstraction> indexLookup = new TreeMap<>();
        indexLookup.put("big_index", indexAbstraction);
        when(metadata.getIndicesLookup()).thenReturn(indexLookup);

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);

        List<ShardIterator> iterators = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node-" + i);
            when(nodes.get("node-" + i)).thenReturn(node);
            ShardRouting shardRouting = mock(ShardRouting.class);
            when(shardRouting.currentNodeId()).thenReturn("node-" + i);
            when(shardRouting.shardId()).thenReturn(new ShardId(new Index("big_index", "uuid-big"), i));
            ShardIterator iter = mock(ShardIterator.class);
            when(iter.nextOrNull()).thenReturn(shardRouting);
            iterators.add(iter);
        }

        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey(), limit).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting opRouting = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(opRouting);
        when(opRouting.searchShards(eq(clusterState), eq(new String[] { "big_index" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(iterators)
        );

        OpenSearchTableScan scan = createScanWithResolution("big_index", resolution);
        ShardTargetResolver resolverUnderTest = new ShardTargetResolver(scan, clusterService);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> resolverUnderTest.resolve(clusterState, null));
        assertTrue(ex.getMessage(), ex.getMessage().contains("big_index"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("[" + shardCount + "] shards"));
        assertTrue(ex.getMessage(), ex.getMessage().contains("action.search.shard_count.limit"));
    }

    /**
     * Nothing is rejected until an operator opts in. {@code action.search.shard_count.limit} defaults
     * to {@code Long.MAX_VALUE}, so an unconfigured cluster fans out freely and the can-match
     * pre-filter plus the per-node dispatch throttle are what bound the work.
     */
    public void testResolveIsUnlimitedByDefault() {
        int shardCount = 5;

        // Carried resolution with a single concrete index.
        IndexMetadata imd = mockIndexMetadata("big_index", "uuid-big");
        IndexResolution resolution = stubResolution("big_index", List.of(imd));

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndicesLookup()).thenReturn(new TreeMap<>());
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

        ClusterService clusterService = mock(ClusterService.class);
        // Settings.EMPTY — the setting is registered but never set, so its default applies.
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting opRouting = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(opRouting);
        when(opRouting.searchShards(eq(clusterState), eq(new String[] { "big_index" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(iterators)
        );

        OpenSearchTableScan scan = createScanWithResolution("big_index", resolution);
        ShardTargetResolver resolverUnderTest = new ShardTargetResolver(scan, clusterService);

        List<ExecutionTarget> targets = resolverUnderTest.resolve(clusterState, null);
        assertEquals("an unconfigured limit rejects nothing", shardCount, targets.size());
    }

    /**
     * When the resolved shard count is exactly at the limit for a multi-index query,
     * resolve() must succeed.
     */
    public void testResolveSucceedsAtExactLimitForAlias() {
        int limit = 3;

        // Carried resolution with two backing indices for the alias.
        IndexMetadata imdA = mockIndexMetadata("idx_a", "uuid-a");
        IndexMetadata imdB = mockIndexMetadata("idx_b", "uuid-b");
        IndexResolution resolution = stubResolution("my_alias", List.of(imdA, imdB));

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);

        IndexAbstraction aliasAbstraction = mock(IndexAbstraction.class);
        when(aliasAbstraction.getType()).thenReturn(IndexAbstraction.Type.ALIAS);
        TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
        lookup.put("my_alias", aliasAbstraction);
        when(metadata.getIndicesLookup()).thenReturn(lookup);

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);

        // Exactly 3 shards across 2 indices — at the limit.
        List<ShardIterator> iterators = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node-" + i);
            when(nodes.get("node-" + i)).thenReturn(node);
            ShardRouting shardRouting = mock(ShardRouting.class);
            when(shardRouting.currentNodeId()).thenReturn("node-" + i);
            String idx = i < 2 ? "idx_a" : "idx_b";
            String uuid = i < 2 ? "uuid-a" : "uuid-b";
            when(shardRouting.shardId()).thenReturn(new ShardId(new Index(idx, uuid), i % 2));
            ShardIterator iter = mock(ShardIterator.class);
            when(iter.nextOrNull()).thenReturn(shardRouting);
            iterators.add(iter);
        }

        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey(), limit).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting opRouting = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(opRouting);
        when(opRouting.searchShards(eq(clusterState), eq(new String[] { "idx_a", "idx_b" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(iterators)
        );

        OpenSearchTableScan scan = createScanWithResolution("my_alias", resolution);
        ShardTargetResolver resolverUnderTest = new ShardTargetResolver(scan, clusterService);

        List<ExecutionTarget> targets = resolverUnderTest.resolve(clusterState, null);
        assertEquals(limit, targets.size());
    }

    // ========== Helpers ==========

    /** Creates an {@link OpenSearchTableScan} carrying a pre-resolved {@link IndexResolution}. */
    private OpenSearchTableScan createScanWithResolution(String tableName, IndexResolution resolution) {
        RelDataType rowType = typeFactory.builder().add("v", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of(tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, cluster.traitSet(), table, List.of("datafusion"), List.of(), null, resolution);
    }

    /** Builds an {@link IndexResolution} for a single concrete index via the literal-name path. */
    private IndexResolution stubResolution(String requestedName, String concreteName) {
        return stubResolution(requestedName, List.of(mockIndexMetadata(concreteName, concreteName + "-uuid")));
    }

    /** Builds an {@link IndexResolution} via a mock cluster state that resolves to the given indices. */
    private IndexResolution stubResolution(String requestedName, List<IndexMetadata> indices) {
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(state.metadata()).thenReturn(metadata);

        if (indices.size() == 1) {
            // Single concrete index path
            IndexMetadata imd = indices.get(0);
            IndexAbstraction abstraction = mock(IndexAbstraction.class);
            when(abstraction.getType()).thenReturn(IndexAbstraction.Type.CONCRETE_INDEX);
            when(abstraction.getIndices()).thenReturn(List.of(imd));
            when(imd.getState()).thenReturn(IndexMetadata.State.OPEN);
            TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
            lookup.put(requestedName, abstraction);
            when(metadata.getIndicesLookup()).thenReturn(lookup);
        } else {
            // Alias path
            for (IndexMetadata imd : indices) {
                when(imd.getState()).thenReturn(IndexMetadata.State.OPEN);
                AliasMetadata aliasMd = mock(AliasMetadata.class);
                when(aliasMd.filteringRequired()).thenReturn(false);
                when(imd.getAliases()).thenReturn(Map.of(requestedName, aliasMd));
            }
            IndexAbstraction aliasAbstraction = mock(IndexAbstraction.class);
            when(aliasAbstraction.getType()).thenReturn(IndexAbstraction.Type.ALIAS);
            when(aliasAbstraction.getIndices()).thenReturn(indices);
            TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
            lookup.put(requestedName, aliasAbstraction);
            when(metadata.getIndicesLookup()).thenReturn(lookup);
        }

        return IndexResolution.resolve(requestedName, state);
    }

    private IndexMetadata mockIndexMetadata(String name, String uuid) {
        IndexMetadata imd = mock(IndexMetadata.class);
        when(imd.getIndex()).thenReturn(new Index(name, uuid));
        when(imd.getNumberOfShards()).thenReturn(1);
        when(imd.getState()).thenReturn(IndexMetadata.State.OPEN);
        return imd;
    }

    private IndexMetadata mockIndexMetadata(String name, String uuid, int shardCount) {
        IndexMetadata imd = mock(IndexMetadata.class);
        when(imd.getIndex()).thenReturn(new Index(name, uuid));
        when(imd.getNumberOfShards()).thenReturn(shardCount);
        when(imd.getState()).thenReturn(IndexMetadata.State.OPEN);
        return imd;
    }

    /** Creates an {@link OpenSearchTableScan} without a carried resolution (backward-compat null case). */
    private OpenSearchTableScan createScanWithoutResolution(String tableName) {
        RelDataType rowType = typeFactory.builder().add("v", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of(tableName));
        when(table.getRowType()).thenReturn(rowType);
        return new OpenSearchTableScan(cluster, cluster.traitSet(), table, List.of("datafusion"), List.of());
    }

    // ========== D4: carried IndexResolution on OpenSearchTableScan ==========

    /**
     * The standard copy(traitSet, inputs) path preserves the carried IndexResolution.
     */
    public void testCopyPreservesCarriedResolution() {
        IndexResolution resolution = stubResolution(
            "my_alias",
            List.of(mockIndexMetadata("idx_a", "idx_a-uuid"), mockIndexMetadata("idx_b", "idx_b-uuid"))
        );
        OpenSearchTableScan scan = createScanWithResolution("my_alias", resolution);

        RelNode copied = scan.copy(scan.getTraitSet(), List.of());
        assertTrue("copy must return OpenSearchTableScan", copied instanceof OpenSearchTableScan);
        OpenSearchTableScan copiedScan = (OpenSearchTableScan) copied;
        assertSame("copy(traitSet,inputs) must preserve carried IndexResolution", resolution, copiedScan.getCarriedResolution());
    }

    /**
     * The copyResolved(backend, children, annotations) path preserves the carried IndexResolution.
     */
    public void testCopyResolvedPreservesCarriedResolution() {
        IndexResolution resolution = stubResolution(
            "my_alias",
            List.of(mockIndexMetadata("idx_a", "idx_a-uuid"), mockIndexMetadata("idx_b", "idx_b-uuid"))
        );
        OpenSearchTableScan scan = createScanWithResolution("my_alias", resolution);

        RelNode copied = scan.copyResolved("datafusion", List.of(), List.of());
        assertTrue("copyResolved must return OpenSearchTableScan", copied instanceof OpenSearchTableScan);
        OpenSearchTableScan copiedScan = (OpenSearchTableScan) copied;
        assertSame("copyResolved must preserve carried IndexResolution", resolution, copiedScan.getCarriedResolution());
    }

    /**
     * stripAnnotations returns this — the field is trivially preserved.
     */
    public void testStripAnnotationsPreservesCarriedResolution() {
        IndexResolution resolution = stubResolution("my_alias", List.of(mockIndexMetadata("idx_a", "idx_a-uuid")));
        OpenSearchTableScan scan = createScanWithResolution("my_alias", resolution);

        RelNode stripped = scan.stripAnnotations(List.of());
        assertSame("stripAnnotations returns this, so resolution is preserved", scan, stripped);
        assertSame(resolution, ((OpenSearchTableScan) stripped).getCarriedResolution());
    }

    /**
     * Regression guard: a multi-index alias carried through planning produces the same
     * shard targets as it did when re-resolving.
     */
    public void testResolveFromCarriedResolutionProducesSameTargets() {
        IndexMetadata imdA = mockIndexMetadata("idx_a", "uuid-a", 2);
        IndexMetadata imdB = mockIndexMetadata("idx_b", "uuid-b", 1);
        IndexResolution resolution = stubResolution("my_alias", List.of(imdA, imdB));
        OpenSearchTableScan scan = createScanWithResolution("my_alias", resolution);

        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);

        List<ShardIterator> iterators = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node-" + i);
            when(nodes.get("node-" + i)).thenReturn(node);
            ShardRouting routing = mock(ShardRouting.class);
            when(routing.currentNodeId()).thenReturn("node-" + i);
            String idx = i < 2 ? "idx_a" : "idx_b";
            String uuid = i < 2 ? "uuid-a" : "uuid-b";
            when(routing.shardId()).thenReturn(new ShardId(new Index(idx, uuid), i < 2 ? i : 0));
            ShardIterator iter = mock(ShardIterator.class);
            when(iter.nextOrNull()).thenReturn(routing);
            iterators.add(iter);
        }

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting opRouting = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(opRouting);
        when(opRouting.searchShards(eq(clusterState), eq(new String[] { "idx_a", "idx_b" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(iterators)
        );

        ShardTargetResolver resolver = new ShardTargetResolver(scan, clusterService);

        List<ExecutionTarget> targets = resolver.resolve(clusterState, null);
        assertEquals("Should produce 3 shard targets from the carried resolution", 3, targets.size());
        for (int i = 0; i < 3; i++) {
            assertEquals("node-" + i, targets.get(i).node().getId());
        }
    }

    /**
     * When the fragment's scan node carries no IndexResolution (null), ShardTargetResolver
     * must throw IllegalStateException.
     */
    public void testThrowsIllegalStateExceptionWhenNoCarriedResolution() {
        OpenSearchTableScan scan = createScanWithoutResolution("my_alias");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> new ShardTargetResolver(scan, clusterService));
        assertTrue(
            "Exception message should name the fragment shape",
            ex.getMessage().contains("OpenSearchTableScan") || ex.getMessage().contains("IndexResolution")
        );
    }

    /**
     * The resolution carried through planning reflects strict IndicesOptions — proving the
     * third re-resolution is genuinely gone.
     */
    public void testCarriedResolutionReflectsStrictOptions() {
        IndexMetadata openIdx = mockIndexMetadata("open_idx", "uuid-open", 1);
        IndexResolution strictResolution = stubResolution("test*", List.of(openIdx));

        OpenSearchTableScan scan = createScanWithResolution("test*", strictResolution);

        assertNotNull("Carried resolution must be non-null", scan.getCarriedResolution());
        assertEquals("test*", scan.getCarriedResolution().requestedName());
        assertEquals(1, scan.getCarriedResolution().concreteIndices().size());
        assertEquals("open_idx", scan.getCarriedResolution().concreteIndices().get(0).getIndex().getName());

        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node-0");
        when(nodes.get("node-0")).thenReturn(node);

        ShardRouting routing = mock(ShardRouting.class);
        when(routing.currentNodeId()).thenReturn("node-0");
        when(routing.shardId()).thenReturn(new ShardId(new Index("open_idx", "uuid-open"), 0));
        ShardIterator iter = mock(ShardIterator.class);
        when(iter.nextOrNull()).thenReturn(routing);

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting opRouting = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(opRouting);
        when(opRouting.searchShards(eq(clusterState), eq(new String[] { "open_idx" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(List.of(iter))
        );

        ShardTargetResolver resolver = new ShardTargetResolver(scan, clusterService);
        List<ExecutionTarget> targets = resolver.resolve(clusterState, null);
        assertEquals(1, targets.size());
        assertEquals("node-0", targets.get(0).node().getId());
    }

    /**
     * When the requested name is an index pattern (wildcard or comma-list) that does not appear
     * in the cluster metadata's indices lookup, describeIndexSource classifies it as
     * "index pattern [name]". The rejection message must surface this wording so operators can
     * identify which expression caused the limit to be hit.
     */
    public void testResolveRejectsIndexPatternExceedingMaxShardsWithPatternWording() {
        int limit = 2;

        // A wildcard pattern resolves to two backing indices but the pattern itself is not
        // in the metadata indices lookup — so describeIndexSource falls through to "index pattern".
        IndexMetadata imdA = mockIndexMetadata("logs-2024-01", "uuid-a");
        IndexMetadata imdB = mockIndexMetadata("logs-2024-02", "uuid-b");
        IndexResolution resolution = stubResolution("logs-*", List.of(imdA, imdB));

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);

        // The pattern "logs-*" is NOT in the lookup — exercising the fallback path.
        TreeMap<String, IndexAbstraction> lookup = new TreeMap<>();
        when(metadata.getIndicesLookup()).thenReturn(lookup);

        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);

        // 4 shards total across the two indices — exceeds limit of 2.
        int shardCount = 4;
        List<ShardIterator> iterators = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            DiscoveryNode node = mock(DiscoveryNode.class);
            when(node.getId()).thenReturn("node-" + i);
            when(nodes.get("node-" + i)).thenReturn(node);
            ShardRouting shardRouting = mock(ShardRouting.class);
            when(shardRouting.currentNodeId()).thenReturn("node-" + i);
            String idx = i < 2 ? "logs-2024-01" : "logs-2024-02";
            String uuid = i < 2 ? "uuid-a" : "uuid-b";
            when(shardRouting.shardId()).thenReturn(new ShardId(new Index(idx, uuid), i % 2));
            ShardIterator iter = mock(ShardIterator.class);
            when(iter.nextOrNull()).thenReturn(shardRouting);
            iterators.add(iter);
        }

        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey(), limit).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, Set.of(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        OperationRouting opRouting = mock(OperationRouting.class);
        when(clusterService.operationRouting()).thenReturn(opRouting);
        when(opRouting.searchShards(eq(clusterState), eq(new String[] { "logs-2024-01", "logs-2024-02" }), any(), any())).thenReturn(
            new GroupShardsIterator<>(iterators)
        );

        OpenSearchTableScan scan = createScanWithResolution("logs-*", resolution);
        ShardTargetResolver resolverUnderTest = new ShardTargetResolver(scan, clusterService);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> resolverUnderTest.resolve(clusterState, null));
        assertTrue(
            "Message must use 'index pattern' wording for wildcard patterns, got: " + ex.getMessage(),
            ex.getMessage().contains("index pattern [logs-*]")
        );
        assertTrue("Message must state shard count", ex.getMessage().contains("[" + shardCount + "] shards"));
        assertTrue("Message must state the limit", ex.getMessage().contains("[" + limit + "]"));
        assertTrue("Message must name the setting", ex.getMessage().contains("action.search.shard_count.limit"));
    }
}
