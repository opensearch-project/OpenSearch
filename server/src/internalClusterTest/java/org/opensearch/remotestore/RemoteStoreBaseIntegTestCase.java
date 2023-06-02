/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.After;
import org.junit.Before;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class RemoteStoreBaseIntegTestCase extends OpenSearchIntegTestCase {
    protected static final String REPOSITORY_NAME = "test-remore-store-repo";
    protected static final int SHARD_COUNT = 1;
    protected static final int REPLICA_COUNT = 1;

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE, "true").build();
    }

    public Settings indexSettings() {
        return defaultIndexSettings();
    }

    private Settings defaultIndexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY, REPOSITORY_NAME)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    protected Settings remoteStoreIndexSettings(int numberOfReplicas) {
        return Settings.builder()
            .put(defaultIndexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .build();
    }

    protected Settings remoteTranslogIndexSettings(int numberOfReplicas) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(numberOfReplicas))
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, REPOSITORY_NAME)
            .build();
    }

    @Before
    public void setup() {
        internalCluster().startClusterManagerOnlyNode();
        Path absolutePath = randomRepoPath().toAbsolutePath();
        assertAcked(
            clusterAdmin().preparePutRepository(REPOSITORY_NAME).setType("fs").setSettings(Settings.builder().put("location", absolutePath))
        );
    }

    @After
    public void teardown() {
        assertAcked(clusterAdmin().prepareDeleteRepository(REPOSITORY_NAME));
    }

}
