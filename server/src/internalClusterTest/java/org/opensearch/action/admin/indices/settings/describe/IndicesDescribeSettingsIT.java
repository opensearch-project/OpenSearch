/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.settings.describe;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.SettingMetadata;
import org.opensearch.common.settings.SettingValidation;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class IndicesDescribeSettingsIT extends OpenSearchIntegTestCase {

    public void testDefinitionsDoNotDependOnIndices() {
        ensureStableCluster(internalCluster().size());
        // Answered on the non-cluster-manager node it is sent to; nothing is forwarded.
        var client = internalCluster().nonClusterManagerClient();
        var request = new IndicesDescribeSettingsRequest(
            "index.number_of_shards",
            "index.number_of_replicas",
            "index.refresh_interval",
            "index.unknown"
        );
        var before = client.execute(IndicesDescribeSettingsAction.INSTANCE, request).actionGet().settings();
        assertEquals(
            Map.of(
                "index.number_of_shards",
                SettingMetadata.withDefault(false, IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getDefaultRaw(Settings.EMPTY))
                    .withValidation(SettingValidation.of("integer", "1", "1024")),
                "index.number_of_replicas",
                SettingMetadata.withDefault(true, "1").withValidation(SettingValidation.of("integer", "0", null)),
                "index.refresh_interval",
                SettingMetadata.withDefault(true, "1s").withValidation(SettingValidation.of("time", "-1", null))
            ),
            before
        );

        // An index configured away from every default changes nothing: the API describes definitions, not values.
        createIndex(
            "test",
            Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("index.refresh_interval", "5s")
                .build()
        );
        ensureGreen("test");
        var after = client.execute(IndicesDescribeSettingsAction.INSTANCE, request).actionGet().settings();
        assertEquals(before, after);
    }

    public void testMergeSchedulerDefaultsFollowTheClusterSetting() throws Exception {
        // index.merge.scheduler.max_thread_count, max_merge_count and auto_throttle declare their default as the current value
        // of the matching cluster.index.merge.scheduler.* setting: MergeSchedulerConfig reads a static that IndexService writes
        // when that cluster setting changes, which only happens on nodes hosting at least one index. The described default
        // therefore follows the cluster setting, as include_defaults does; every other definition is unaffected. This pins
        // that behaviour so it is deliberate rather than accidental.
        //
        // Two limits of this test. The statics live for the JVM, so a previous test class may have left them set; every baseline
        // is read from the response rather than assumed, and only the deltas and the restoration are asserted. And every node
        // of the internal cluster runs in this one JVM, so they share one copy of the statics: the test cannot observe the
        // production case where an index-free node keeps reporting the processor-derived default while a data node reports the
        // cluster value. It pins the direction of the dependency, not its per-node scope.
        ensureStableCluster(internalCluster().size());
        createIndex("merge_defaults");
        ensureGreen("merge_defaults");
        var client = internalCluster().nonClusterManagerClient();
        var everything = new IndicesDescribeSettingsRequest("index.*");
        var baseline = client.execute(IndicesDescribeSettingsAction.INSTANCE, everything).actionGet().settings();
        String baselineThreads = baseline.get("index.merge.scheduler.max_thread_count").defaultValue();
        String baselineMerges = baseline.get("index.merge.scheduler.max_merge_count").defaultValue();
        String baselineThrottle = baseline.get("index.merge.scheduler.auto_throttle").defaultValue();
        // Pick values that cannot collide with whatever the baseline is, so the assertions below prove a change happened.
        int threads = Integer.parseInt(baselineThreads) + 3;
        int merges = Integer.parseInt(baselineMerges) + 7;
        boolean throttle = Boolean.parseBoolean(baselineThrottle) == false;

        try {
            client.admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put("cluster.index.merge.scheduler.max_thread_count", threads)
                        .put("cluster.index.merge.scheduler.max_merge_count", merges)
                        .put("cluster.index.merge.scheduler.auto_throttle", throttle)
                )
                .get();
            // The write happens on the cluster-settings applier thread of each node; poll rather than assume.
            assertBusy(() -> {
                var updated = client.execute(IndicesDescribeSettingsAction.INSTANCE, everything).actionGet().settings();
                assertEquals(Integer.toString(threads), updated.get("index.merge.scheduler.max_thread_count").defaultValue());
                assertEquals(Integer.toString(merges), updated.get("index.merge.scheduler.max_merge_count").defaultValue());
                assertEquals(Boolean.toString(throttle), updated.get("index.merge.scheduler.auto_throttle").defaultValue());
                var rest = new java.util.HashMap<>(updated);
                var expectedRest = new java.util.HashMap<>(baseline);
                for (String key : List.of(
                    "index.merge.scheduler.max_thread_count",
                    "index.merge.scheduler.max_merge_count",
                    "index.merge.scheduler.auto_throttle"
                )) {
                    rest.remove(key);
                    expectedRest.remove(key);
                }
                assertEquals(expectedRest, rest);
            });
        } finally {
            client.admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .putNull("cluster.index.merge.scheduler.max_thread_count")
                        .putNull("cluster.index.merge.scheduler.max_merge_count")
                        .putNull("cluster.index.merge.scheduler.auto_throttle")
                )
                .get();
        }
        // Resetting the cluster settings restores whatever the baseline was.
        assertBusy(() -> {
            var reset = client.execute(IndicesDescribeSettingsAction.INSTANCE, everything).actionGet().settings();
            assertEquals(baselineThreads, reset.get("index.merge.scheduler.max_thread_count").defaultValue());
            assertEquals(baselineMerges, reset.get("index.merge.scheduler.max_merge_count").defaultValue());
            assertEquals(baselineThrottle, reset.get("index.merge.scheduler.auto_throttle").defaultValue());
        });
    }
}
