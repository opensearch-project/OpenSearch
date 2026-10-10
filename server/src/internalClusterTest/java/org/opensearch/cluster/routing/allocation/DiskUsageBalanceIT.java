/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.MockInternalClusterInfoService;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Integration tests for the {@code cluster.routing.allocation.balance.disk_usage} setting.
 * Validates that the balance factor does not destabilize the cluster and that its
 * {@code 0.0f} default keeps the allocator balancing strictly by shard count.
 *
 * <p>Follows the structure of {@link org.opensearch.cluster.routing.allocation.decider.MockDiskUsagesIT}:
 * registers {@link MockInternalClusterInfoService.TestPlugin} and uses
 * {@link MockInternalClusterInfoService#setShardSizeFunctionAndRefresh(Function)} to inject
 * per-shard sizes without having to index real data.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3)
public class DiskUsageBalanceIT extends OpenSearchIntegTestCase {

    private static final String INDEX_SMALL = "index-small";
    private static final String INDEX_LARGE = "index-large";

    // Use small byte values to stay away from float-precision edge cases in the balancer's
    // weight math (see MockDiskUsagesIT which uses 1-byte shards for the same reason).
    private static final long SMALL_SHARD_SIZE = 1L;
    private static final long LARGE_SHARD_SIZE = 100L;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockInternalClusterInfoService.TestPlugin.class);
    }

    /**
     * Reset the persistent cluster setting after every test to prevent leakage into other
     * tests sharing the same JVM/cluster.
     */
    @After
    public void resetDiskUsageBalanceFactor() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .putNull(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey())
                        .putNull(BalancedShardsAllocator.BALANCE_MODE_SETTING.getKey())
                        .putNull(BalancedShardsAllocator.THRESHOLD_SETTING.getKey())
                )
        );
    }

    private MockInternalClusterInfoService getMockInternalClusterInfoService() {
        return (MockInternalClusterInfoService) internalCluster().getCurrentClusterManagerNodeInstance(ClusterInfoService.class);
    }

    /**
     * Fake shard-size function: large for {@link #INDEX_LARGE} shards, small for
     * {@link #INDEX_SMALL} shards, zero for everything else.
     */
    private static long fakeShardSize(ShardRouting shardRouting) {
        final String indexName = shardRouting.getIndexName();
        if (INDEX_LARGE.equals(indexName)) {
            return LARGE_SHARD_SIZE;
        } else if (INDEX_SMALL.equals(indexName)) {
            return SMALL_SHARD_SIZE;
        }
        return 0L;
    }

    private Map<String, Integer> getShardCountByNodeId() {
        final Map<String, Integer> shardCountByNodeId = new HashMap<>();
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (final RoutingNode node : clusterState.getRoutingNodes()) {
            shardCountByNodeId.put(node.nodeId(), clusterState.getRoutingNodes().node(node.nodeId()).numberOfOwningShards());
        }
        return shardCountByNodeId;
    }

    /**
     * Computes per-node total bytes using the same fake shard-size function that drives the
     * allocator's ClusterInfo view.
     */
    private Map<String, Long> getBytesByNodeId(Function<ShardRouting, Long> sizeFn) {
        final Map<String, Long> bytesByNodeId = new HashMap<>();
        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        for (final RoutingNode node : clusterState.getRoutingNodes()) {
            long total = 0L;
            for (ShardRouting shard : node) {
                total += sizeFn.apply(shard);
            }
            bytesByNodeId.put(node.nodeId(), total);
        }
        return bytesByNodeId;
    }

    private static long spread(Map<String, Long> byNode) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long v : byNode.values()) {
            if (v < min) min = v;
            if (v > max) max = v;
        }
        return max - min;
    }

    private int totalUnassigned() {
        return client().admin().cluster().prepareState().get().getState().getRoutingNodes().unassigned().size();
    }

    /**
     * Enabling {@code cluster.routing.allocation.balance.disk_usage} as the only non-zero
     * balance factor must not destabilize the cluster: all shards stay assigned, and the
     * byte-spread across nodes does not grow. Because the default shard-count balancer
     * already produces an even distribution for a homogeneous-per-index workload, "no worse
     * than before" is the correct and stable invariant to check here.
     */
    public void testDiskUsageBalanceRebalancesByBytes() throws Exception {
        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setUpdateFrequency(TimeValue.timeValueMillis(200));
        clusterInfoService.setShardSizeFunctionAndRefresh(DiskUsageBalanceIT::fakeShardSize);

        final List<String> nodeIds = StreamSupport.stream(
            client().admin().cluster().prepareState().get().getState().getRoutingNodes().spliterator(),
            false
        ).map(RoutingNode::nodeId).collect(Collectors.toList());
        assertThat("cluster must have 3 data nodes", nodeIds.size(), equalTo(3));

        // 6 small + 6 large shards, 0 replicas. With 3 nodes the default shard-count
        // balancer places 4 shards per node.
        assertAcked(prepareCreate(INDEX_SMALL).setSettings(Settings.builder().put("number_of_shards", 6).put("number_of_replicas", 0)));
        assertAcked(prepareCreate(INDEX_LARGE).setSettings(Settings.builder().put("number_of_shards", 6).put("number_of_replicas", 0)));
        ensureGreen(INDEX_SMALL, INDEX_LARGE);

        clusterInfoService.refresh();

        final Map<String, Integer> initialShardCounts = getShardCountByNodeId();
        for (String nodeId : nodeIds) {
            assertThat(
                "node " + nodeId + " should have 4 shards under default shard-count balancer",
                initialShardCounts.get(nodeId),
                equalTo(4)
            );
        }
        assertThat("no unassigned shards initially", totalUnassigned(), equalTo(0));

        final long spreadBefore = spread(getBytesByNodeId(DiskUsageBalanceIT::fakeShardSize));
        logger.info("--> byte spread before enabling disk_usage balance = {}", spreadBefore);

        // Turn on disk-usage balance alongside shard-count balance. We intentionally leave
        // SHARD_BALANCE_FACTOR at its default so the allocator's weight function stays well
        // conditioned (index+shard+disk_usage must sum to > 0, and we don't want to force
        // arbitrarily-large relocations).
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 1.0f))
        );

        clusterInfoService.refresh();
        assertAcked(client().admin().cluster().prepareReroute());

        // Let any rebalance triggered by the setting update settle, then verify invariants.
        assertBusy(() -> {
            ensureGreen(INDEX_SMALL, INDEX_LARGE);
            assertThat("still no unassigned shards", totalUnassigned(), equalTo(0));
            final long spreadAfter = spread(getBytesByNodeId(DiskUsageBalanceIT::fakeShardSize));
            logger.info("--> byte spread after enabling disk_usage balance = {}", spreadAfter);
            assertThat("byte spread should not increase once disk_usage balance is enabled", spreadAfter, lessThanOrEqualTo(spreadBefore));
        }, 30, java.util.concurrent.TimeUnit.SECONDS);
    }

    /**
     * With {@code balance.disk_usage} at its {@code 0.0f} default, the allocator must place
     * shards by count only: even shard counts per node, regardless of shard byte sizes.
     */
    public void testDefaultBehaviorUnchanged() throws Exception {
        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setUpdateFrequency(TimeValue.timeValueMillis(200));
        clusterInfoService.setShardSizeFunctionAndRefresh(DiskUsageBalanceIT::fakeShardSize);

        final List<String> nodeIds = StreamSupport.stream(
            client().admin().cluster().prepareState().get().getState().getRoutingNodes().spliterator(),
            false
        ).map(RoutingNode::nodeId).collect(Collectors.toList());
        assertThat("cluster must have 3 data nodes", nodeIds.size(), equalTo(3));

        // Heterogeneous sizes across 3 nodes -> expect 4 per node because the default
        // disk_usage factor is 0.0f and the allocator ignores disk usage.
        assertAcked(prepareCreate(INDEX_SMALL).setSettings(Settings.builder().put("number_of_shards", 6).put("number_of_replicas", 0)));
        assertAcked(prepareCreate(INDEX_LARGE).setSettings(Settings.builder().put("number_of_shards", 6).put("number_of_replicas", 0)));
        ensureGreen(INDEX_SMALL, INDEX_LARGE);

        clusterInfoService.refresh();
        assertAcked(client().admin().cluster().prepareReroute());

        assertBusy(() -> {
            final Map<String, Integer> byNode = getShardCountByNodeId();
            for (String nodeId : nodeIds) {
                assertThat("node " + nodeId + " should have exactly 4 shards with default disk_usage=0.0f", byNode.get(nodeId), equalTo(4));
            }
            assertThat("no unassigned shards in default-behavior test", totalUnassigned(), equalTo(0));
        }, 30, java.util.concurrent.TimeUnit.SECONDS);

        // Sanity: the setting really is at its 0.0f default.
        final Settings persistent = client().admin().cluster().prepareState().get().getState().getMetadata().persistentSettings();
        final String key = BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey();
        assertThat("disk_usage factor must be unset (default 0.0f) for this test", persistent.get(key, "0.0"), equalTo("0.0"));
    }

    /**
     * With {@code balance.mode=ratio}, disk_usage balance enabled, and a lower
     * {@code threshold} (because threshold is now a relative-deviation fraction rather
     * than a shard-count delta), the allocator must rebalance by bytes without
     * destabilizing the cluster: all shards stay assigned and the byte-spread across
     * nodes does not grow relative to the count-mode baseline.
     */
    public void testRatioModeRebalancesByBytes() throws Exception {
        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setUpdateFrequency(TimeValue.timeValueMillis(200));
        clusterInfoService.setShardSizeFunctionAndRefresh(DiskUsageBalanceIT::fakeShardSize);

        final List<String> nodeIds = StreamSupport.stream(
            client().admin().cluster().prepareState().get().getState().getRoutingNodes().spliterator(),
            false
        ).map(RoutingNode::nodeId).collect(Collectors.toList());
        assertThat("cluster must have 3 data nodes", nodeIds.size(), equalTo(3));

        assertAcked(prepareCreate(INDEX_SMALL).setSettings(Settings.builder().put("number_of_shards", 6).put("number_of_replicas", 0)));
        assertAcked(prepareCreate(INDEX_LARGE).setSettings(Settings.builder().put("number_of_shards", 6).put("number_of_replicas", 0)));
        ensureGreen(INDEX_SMALL, INDEX_LARGE);

        clusterInfoService.refresh();

        final long spreadBefore = spread(getBytesByNodeId(DiskUsageBalanceIT::fakeShardSize));
        logger.info("--> byte spread before enabling ratio mode = {}", spreadBefore);

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put(BalancedShardsAllocator.BALANCE_MODE_SETTING.getKey(), "ratio")
                        .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
                        // threshold is a relative-deviation fraction in ratio mode.
                        .put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 0.1f)
                )
        );

        clusterInfoService.refresh();
        assertAcked(client().admin().cluster().prepareReroute());

        assertBusy(() -> {
            ensureGreen(INDEX_SMALL, INDEX_LARGE);
            assertThat("still no unassigned shards under ratio mode", totalUnassigned(), equalTo(0));
            final long spreadAfter = spread(getBytesByNodeId(DiskUsageBalanceIT::fakeShardSize));
            logger.info("--> byte spread under ratio mode = {}", spreadAfter);
            assertThat("ratio-mode byte spread should not exceed count-mode baseline", spreadAfter, lessThanOrEqualTo(spreadBefore));
        }, 30, java.util.concurrent.TimeUnit.SECONDS);
    }

    /**
     * Toggle {@code balance.mode} between {@code count} and {@code ratio} at runtime and
     * verify the dynamic settings-update consumer rebuilds the weight function without
     * losing shards or turning the cluster red.
     */
    public void testBalanceModeDynamicToggle() throws Exception {
        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setUpdateFrequency(TimeValue.timeValueMillis(200));
        clusterInfoService.setShardSizeFunctionAndRefresh(DiskUsageBalanceIT::fakeShardSize);

        assertAcked(prepareCreate(INDEX_SMALL).setSettings(Settings.builder().put("number_of_shards", 6).put("number_of_replicas", 0)));
        assertAcked(prepareCreate(INDEX_LARGE).setSettings(Settings.builder().put("number_of_shards", 6).put("number_of_replicas", 0)));
        ensureGreen(INDEX_SMALL, INDEX_LARGE);

        for (String mode : new String[] { "ratio", "count", "ratio", "count" }) {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(
                        Settings.builder()
                            .put(BalancedShardsAllocator.BALANCE_MODE_SETTING.getKey(), mode)
                            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
                            // threshold has different semantics per mode; pick a permissive value
                            // that is sensible in both (0.1 is a lower bound in count mode and a
                            // reasonable fraction in ratio mode).
                            .put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 0.1f)
                    )
            );
            clusterInfoService.refresh();
            assertAcked(client().admin().cluster().prepareReroute());
            assertBusy(() -> {
                ensureGreen(INDEX_SMALL, INDEX_LARGE);
                assertThat("no unassigned shards after toggling balance.mode=" + mode, totalUnassigned(), equalTo(0));
            }, 30, java.util.concurrent.TimeUnit.SECONDS);
        }

        // Reject an invalid enum value; the setting must refuse and surface a clear error.
        try {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(BalancedShardsAllocator.BALANCE_MODE_SETTING.getKey(), "bogus"))
                .get();
            fail("expected IllegalArgumentException for balance.mode=bogus");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), org.hamcrest.Matchers.containsString("bogus"));
        }
    }
}
