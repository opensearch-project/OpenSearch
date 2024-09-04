/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.settings;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.cluster.routing.allocation.decider.SearchReplicaAllocationDecider.SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SearchOnlyReplicaFeatureFlagIT extends OpenSearchIntegTestCase {

    private static final String TEST_INDEX = "test_index";

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, Boolean.FALSE)
            .build();
    }

    public void testCreateFeatureFlagDisabled() {
        Settings settings = Settings.builder().put(indexSettings()).put(FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL, false).build();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> createIndex(TEST_INDEX, settings));
        assertTrue(exception.getMessage().contains("unknown setting"));
    }

    public void testUpdateFeatureFlagDisabled() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();

        createIndex(TEST_INDEX, settings);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            client().admin()
                .indices()
                .prepareUpdateSettings(TEST_INDEX)
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1))
                .get();
        });
        assertTrue(exception.getMessage().contains("unknown setting"));
    }

    public void testFilterAllocationSettingNotRegistered() {
        expectThrows(IllegalArgumentException.class, () -> {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(SEARCH_REPLICA_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_name", "node"))
                .execute()
                .actionGet();
        });
    }
}
