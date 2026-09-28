/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.tiering;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.index.IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.index.IndexModule.IS_WARM_INDEX_SETTING;
import static org.opensearch.index.IndexModule.TieringState.HOT;
import static org.opensearch.index.IndexModule.TieringState.HOT_TO_WARM;
import static org.opensearch.index.IndexModule.TieringState.WARM;
import static org.opensearch.index.IndexModule.TieringState.WARM_TO_HOT;

public class TieringTestUtils {

    /**
     * Listener that waits for migration to be removed for an index and counts down latch
     */
    public static class MockTieringCompletionListener implements ClusterStateListener {
        protected final CountDownLatch countDownLatch;
        protected final String index;
        protected final String migrationType;

        public MockTieringCompletionListener(final CountDownLatch latch, final String index, String migrationType) {
            this.countDownLatch = latch;
            this.index = index;
            this.migrationType = migrationType;
        }

        protected boolean isShardInWarmNode(final ShardRouting shard, final ClusterState clusterState) {
            if (shard.unassigned()) {
                return false;
            }
            final boolean isShardFoundOnWarmNode = clusterState.getNodes().get(shard.currentNodeId()).isWarmNode();
            return shard.started() && isShardFoundOnWarmNode;
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (!event.state().routingTable().hasIndex(index)) {
                return;
            }
            List<ShardRouting> CurrentShardRoutings = event.state().routingTable().allShards(index);

            boolean relocationCompleted = true;
            for (ShardRouting shard : CurrentShardRoutings) {
                if (migrationType.equals(HOT_TO_WARM.toString()) && !isShardInWarmNode(shard, event.state())
                    || migrationType.equals(WARM_TO_HOT.toString()) && isShardInWarmNode(shard, event.state())) {
                    relocationCompleted = false;
                    break;
                }
            }
            if (relocationCompleted) {
                countDownLatch.countDown();
            }
        }
    }

    /**
     * Verifies warm index settings after migration
     * @return Map of setting key-value pairs that should be validated in the test
     */
    public static Map<String, Object> getExpectedWarmIndexSettings() {
        Map<String, Object> expectedSettings = new HashMap<>();
        expectedSettings.put(IS_WARM_INDEX_SETTING.getKey(), true);
        expectedSettings.put(INDEX_TIERING_STATE.getKey(), WARM);
        expectedSettings.put(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), "tiered-storage");
        return expectedSettings;
    }

    /**
     * Verifies warm index settings after migration
     * @return Map of setting key-value pairs that should be validated in the test
     */
    public static Map<String, Object> getExpectedHotIndexSettings() {
        Map<String, Object> expectedSettings = new HashMap<>();
        expectedSettings.put(IS_WARM_INDEX_SETTING.getKey(), false);
        expectedSettings.put(INDEX_TIERING_STATE.getKey(), HOT);
        expectedSettings.put(INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), "default");
        return expectedSettings;
    }

    public static Settings buildDisabledAllocationSettings() {
        return Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 0)
            .put("cluster.routing.allocation.node_concurrent_incoming_recoveries", 0)
            .put("cluster.routing.allocation.node_concurrent_outgoing_recoveries", 0)
            .build();
    }

    public static Settings buildEnabledAllocationSettings(int concurrentRecoveries) {
        return Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", concurrentRecoveries)
            .put("cluster.routing.allocation.node_concurrent_incoming_recoveries", concurrentRecoveries)
            .put("cluster.routing.allocation.node_concurrent_outgoing_recoveries", concurrentRecoveries)
            .build();
    }

    @SuppressForbidden(reason = "Best effort sleep to allow async tiering operations to proceed")
    public static void safeSleep(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }
}
