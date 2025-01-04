/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

public class AutoExpandWithSearchReplicaIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-idx-1";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, true).build();
    }

    public void testEnableAutoExpandWhenSearchReplicaActive() {
        internalCluster().startDataOnlyNodes(3);
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 1)
                .put("number_of_search_only_replicas", 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureGreen(INDEX_NAME);

        expectThrows(IllegalArgumentException.class, () -> {
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put("index.auto_expand_replicas", "0-1"))
                .execute()
                .actionGet();
        });

        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put("index.number_of_search_only_replicas", "0"))
            .execute()
            .actionGet();

        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put("index.auto_expand_replicas", "0-1"))
            .execute()
            .actionGet();

        GetSettingsResponse response = client().admin().indices().prepareGetSettings(INDEX_NAME).execute().actionGet();

        assertEquals("0-1", response.getSetting(INDEX_NAME, "index.auto_expand_replicas"));
    }

    public void testEnableSearchReplicaWithAutoExpandActive() {
        internalCluster().startDataOnlyNodes(3);
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureGreen(INDEX_NAME);

        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put("index.auto_expand_replicas", "0-1"))
            .execute()
            .actionGet();

        expectThrows(IllegalArgumentException.class, () -> {
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put("index.number_of_search_only_replicas", "1"))
                .execute()
                .actionGet();
        });

        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put("index.auto_expand_replicas", false))
            .execute()
            .actionGet();

        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put("index.number_of_search_only_replicas", "1"))
            .execute()
            .actionGet();

        GetSettingsResponse response = client().admin().indices().prepareGetSettings(INDEX_NAME).execute().actionGet();

        assertEquals("1", response.getSetting(INDEX_NAME, "index.number_of_search_only_replicas"));
    }
}
