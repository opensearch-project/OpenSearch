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
import org.opensearch.transport.client.Client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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
        // of the matching cluster.index.merge.scheduler.* setting: MergeSchedulerConfig reads statics that IndexService writes
        // when that cluster setting changes, which only happens on nodes hosting at least one index. The described default
        // therefore follows the cluster setting, as include_defaults does; every other definition is unaffected. This pins
        // that behaviour so it is deliberate rather than accidental.
        //
        // Those statics are JVM-wide, and every node of the internal cluster runs in this one JVM, so they share one copy.
        // While the cluster setting is absent each node derives its own processor-based value and the last node to apply
        // wins, so the absent-setting default has no single well-defined value here and nothing can be captured as a
        // baseline. The test instead pins the cluster setting to one explicit value, moves it to a second, and asserts
        // only the move; clearing the settings at the end is cleanup, not an assertion. For the same reason the test cannot
        // observe the production case where an index-free node keeps reporting the processor-derived default while a data
        // node reports the cluster value: it pins the direction of the dependency, not its per-node scope.
        ensureStableCluster(internalCluster().size());
        createIndex("merge_defaults");
        ensureGreen("merge_defaults");
        var client = internalCluster().nonClusterManagerClient();
        var everything = new IndicesDescribeSettingsRequest("index.*");

        try {
            var pinned = awaitMergeSchedulerDefaults(client, everything, 6, 11, true);
            var moved = awaitMergeSchedulerDefaults(client, everything, 9, 18, false);
            // Nothing else moved with them. Outside assertBusy: an unrelated difference is a failure to report at once, not a
            // condition to wait on.
            assertEquals(withoutMergeSchedulerKeys(pinned), withoutMergeSchedulerKeys(moved));
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
    }

    /** Sets the three cluster settings, then polls the describe API until it reports them as the declared defaults. */
    private static Map<String, SettingMetadata> awaitMergeSchedulerDefaults(
        Client client,
        IndicesDescribeSettingsRequest request,
        int threads,
        int merges,
        boolean throttle
    ) throws Exception {
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
        // The statics are written on each node's cluster-settings applier thread; poll rather than assume.
        var seen = new AtomicReference<Map<String, SettingMetadata>>();
        assertBusy(() -> {
            var settings = client.execute(IndicesDescribeSettingsAction.INSTANCE, request).actionGet().settings();
            assertEquals(Integer.toString(threads), settings.get("index.merge.scheduler.max_thread_count").defaultValue());
            assertEquals(Integer.toString(merges), settings.get("index.merge.scheduler.max_merge_count").defaultValue());
            assertEquals(Boolean.toString(throttle), settings.get("index.merge.scheduler.auto_throttle").defaultValue());
            seen.set(settings);
        });
        return seen.get();
    }

    private static Map<String, SettingMetadata> withoutMergeSchedulerKeys(Map<String, SettingMetadata> settings) {
        var rest = new HashMap<>(settings);
        rest.keySet()
            .removeAll(
                List.of(
                    "index.merge.scheduler.max_thread_count",
                    "index.merge.scheduler.max_merge_count",
                    "index.merge.scheduler.auto_throttle"
                )
            );
        return rest;
    }
}
