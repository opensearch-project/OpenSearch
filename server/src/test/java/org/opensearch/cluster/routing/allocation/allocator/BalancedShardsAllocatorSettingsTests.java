/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.allocator;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING;
import static org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING;

public class BalancedShardsAllocatorSettingsTests extends OpenSearchTestCase {

    public void testBothBalanceFactorsZeroIsRejectedOnConstruction() {
        final Settings settings = Settings.builder()
            .put(INDEX_BALANCE_FACTOR_SETTING.getKey(), "0.0")
            .put(SHARD_BALANCE_FACTOR_SETTING.getKey(), "0.0")
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new BalancedShardsAllocator(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        );
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("must sum to a value greater than zero"));
    }

    public void testBothBalanceFactorsZeroIsRejectedOnDynamicUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new BalancedShardsAllocator(Settings.EMPTY, clusterSettings);

        final Settings newSettings = Settings.builder()
            .put(INDEX_BALANCE_FACTOR_SETTING.getKey(), "0.0")
            .put(SHARD_BALANCE_FACTOR_SETTING.getKey(), "0.0")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("illegal value can't update"));
        assertNotNull(e.getCause());
        assertThat(e.getCause().getMessage(), org.hamcrest.Matchers.containsString("must sum to a value greater than zero"));
    }

    public void testIndexBalanceZeroWithNonZeroShardBalanceIsAccepted() {
        final Settings settings = Settings.builder()
            .put(INDEX_BALANCE_FACTOR_SETTING.getKey(), "0.0")
            .put(SHARD_BALANCE_FACTOR_SETTING.getKey(), "1.0")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(settings, clusterSettings);
        assertEquals(0.0f, allocator.getIndexBalance(), 0.0f);
        assertEquals(1.0f, allocator.getShardBalance(), 0.0f);
    }

    public void testShardBalanceZeroWithNonZeroIndexBalanceIsAccepted() {
        final Settings settings = Settings.builder()
            .put(INDEX_BALANCE_FACTOR_SETTING.getKey(), "1.0")
            .put(SHARD_BALANCE_FACTOR_SETTING.getKey(), "0.0")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(settings, clusterSettings);
        assertEquals(1.0f, allocator.getIndexBalance(), 0.0f);
        assertEquals(0.0f, allocator.getShardBalance(), 0.0f);
    }

    public void testDynamicUpdateIndexBalanceToZeroWhileShardBalanceAlreadyZeroIsRejected() {
        final Settings initialSettings = Settings.builder()
            .put(SHARD_BALANCE_FACTOR_SETTING.getKey(), "0.0")
            .put(INDEX_BALANCE_FACTOR_SETTING.getKey(), "1.0")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(initialSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new BalancedShardsAllocator(initialSettings, clusterSettings);

        final Settings newSettings = Settings.builder().put(INDEX_BALANCE_FACTOR_SETTING.getKey(), "0.0").build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        assertThat(e.getCause().getMessage(), org.hamcrest.Matchers.containsString("must sum to a value greater than zero"));
    }

    public void testDynamicUpdateShardBalanceToZeroWhileIndexBalanceAlreadyZeroIsRejected() {
        final Settings initialSettings = Settings.builder()
            .put(INDEX_BALANCE_FACTOR_SETTING.getKey(), "0.0")
            .put(SHARD_BALANCE_FACTOR_SETTING.getKey(), "1.0")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(initialSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new BalancedShardsAllocator(initialSettings, clusterSettings);

        final Settings newSettings = Settings.builder().put(SHARD_BALANCE_FACTOR_SETTING.getKey(), "0.0").build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        assertThat(e.getCause().getMessage(), org.hamcrest.Matchers.containsString("must sum to a value greater than zero"));
    }

    public void testValidDynamicUpdateIsAccepted() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(Settings.EMPTY, clusterSettings);

        final Settings newSettings = Settings.builder()
            .put(INDEX_BALANCE_FACTOR_SETTING.getKey(), "0.3")
            .put(SHARD_BALANCE_FACTOR_SETTING.getKey(), "0.7")
            .build();

        clusterSettings.applySettings(newSettings);
        assertEquals(0.3f, allocator.getIndexBalance(), 0.001f);
        assertEquals(0.7f, allocator.getShardBalance(), 0.001f);
    }
}
