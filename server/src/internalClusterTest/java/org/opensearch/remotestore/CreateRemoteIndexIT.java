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
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_BUFFER_INTERVAL;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STORE_REPOSITORY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STORE_ENABLED_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_STORE_ENABLED_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class CreateRemoteIndexIT extends OpenSearchIntegTestCase {

    @After
    public void teardown() {
        assertAcked(clusterAdmin().prepareDeleteRepository("my-segment-repo-1"));
        assertAcked(clusterAdmin().prepareDeleteRepository("my-translog-repo-1"));
        assertAcked(clusterAdmin().prepareDeleteRepository("my-custom-repo"));
    }

    @Override
    protected Settings nodeSettings(int nodeOriginal) {
        Settings settings = super.nodeSettings(nodeOriginal);
        Settings.Builder builder = Settings.builder()
            .put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(CLUSTER_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(CLUSTER_REMOTE_STORE_REPOSITORY_SETTING.getKey(), "my-segment-repo-1")
            .put(CLUSTER_REMOTE_TRANSLOG_STORE_ENABLED_SETTING.getKey(), true)
            .put(CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING.getKey(), "my-translog-repo-1")
            .put(settings);
        return builder.build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL, "true")
            .put(FeatureFlags.REMOTE_STORE, "true")
            .build();
    }

    @Before
    public void setup() {
        FeatureFlagSetter.set(FeatureFlags.REMOTE_STORE);
        internalCluster().startClusterManagerOnlyNode();
        assertAcked(
            clusterAdmin().preparePutRepository("my-segment-repo-1")
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath().toAbsolutePath()))
        );
        assertAcked(
            clusterAdmin().preparePutRepository("my-translog-repo-1")
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath().toAbsolutePath()))
        );
        assertAcked(
            clusterAdmin().preparePutRepository("my-custom-repo")
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath().toAbsolutePath()))
        );
    }

    public void testDefaultRemoteStoreNoUserOverride() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices("test-idx-1").includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get("test-idx-1");
        verifyRemoteStoreIndexSettings(
            indexSettings,
            "true",
            "my-segment-repo-1",
            "true",
            "my-translog-repo-1",
            ReplicationType.SEGMENT.toString(),
            null
        );
    }

    public void testRemoteStoreDisabledByUser() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REMOTE_STORE_ENABLED, false)
            .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices("test-idx-1").includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get("test-idx-1");
        verifyRemoteStoreIndexSettings(indexSettings, "false", null, null, null, null, null);
    }

    public void testRemoteStoreEnabledByUserWithoutRemoteRepoAndSegmentReplicationIllegalArgumentException() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REMOTE_STORE_ENABLED, true)
            .build();

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString("To enable index.remote_store.enabled, index.replication.type should be set to SEGMENT")
        );
    }

    public void testRemoteStoreEnabledByUserWithoutRemoteRepoIllegalArgumentException() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SETTING_REMOTE_STORE_ENABLED, true)
            .build();

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString("Setting index.remote_store.repository should be provided with non-empty repository ID")
        );
    }

    public void testReplicationTypeDocumentByUser() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
            .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices("test-idx-1").includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get("test-idx-1");
        verifyRemoteStoreIndexSettings(indexSettings, null, null, null, null, ReplicationType.DOCUMENT.toString(), null);
    }

    public void testRemoteStoreSegmentRepoWithoutRemoteEnabledAndSegmentReplicationIllegalArgumentException() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REMOTE_STORE_REPOSITORY, "my-custom-repo")
            .build();
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString("Settings index.remote_store.repository can only be set/enabled when index.remote_store.enabled is set to true")
        );
    }

    public void testRemoteStoreEnabledByUserWithRemoteRepo() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SETTING_REMOTE_STORE_ENABLED, true)
            .put(SETTING_REMOTE_STORE_REPOSITORY, "my-custom-repo")
            .build();

        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices("test-idx-1").includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get("test-idx-1");
        verifyRemoteStoreIndexSettings(
            indexSettings,
            "true",
            "my-custom-repo",
            "true",
            "my-translog-repo-1",
            ReplicationType.SEGMENT.toString(),
            null
        );
    }

    public void testRemoteStoreTranslogDisabledByUser() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REMOTE_TRANSLOG_STORE_ENABLED, false)
            .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices("test-idx-1").includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get("test-idx-1");
        verifyRemoteStoreIndexSettings(indexSettings, "true", "my-segment-repo-1", "false", null, ReplicationType.SEGMENT.toString(), null);
    }

    public void testRemoteStoreOverrideOnlyTranslogRepoIllegalArgumentException() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "my-custom-repo")
            .build();
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString(
                "Settings index.remote_store.translog.repository can only be set/enabled when index.remote_store.translog.enabled is set to true"
            )
        );
    }

    public void testRemoteStoreOverrideOnlyTranslogEnabled() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REMOTE_TRANSLOG_STORE_ENABLED, true)
            .build();

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString(
                "Settings index.remote_store.translog.enabled can only be set/enabled when index.remote_store.enabled is set to true"
            )
        );
    }

    public void testRemoteStoreOverrideOnlyTranslogEnabledAndRepo() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REMOTE_TRANSLOG_STORE_ENABLED, true)
            .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "my-custom-repo")
            .build();

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString(
                "Settings index.remote_store.translog.enabled can only be set/enabled when index.remote_store.enabled is set to true"
            )
        );
    }

    public void testRemoteStoreOverrideTranslogDisabledCorrectly() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SETTING_REMOTE_STORE_ENABLED, true)
            .put(SETTING_REMOTE_STORE_REPOSITORY, "my-custom-repo")
            .put(SETTING_REMOTE_TRANSLOG_STORE_ENABLED, false)
            .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices("test-idx-1").includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get("test-idx-1");
        verifyRemoteStoreIndexSettings(indexSettings, "true", "my-custom-repo", "false", null, ReplicationType.SEGMENT.toString(), null);
    }

    public void testRemoteStoreOverrideTranslogDisabledWithTranslogRepoIllegalArgumentException() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SETTING_REMOTE_STORE_ENABLED, true)
            .put(SETTING_REMOTE_STORE_REPOSITORY, "my-custom-repo")
            .put(SETTING_REMOTE_TRANSLOG_STORE_ENABLED, false)
            .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "my-custom-repo")
            .build();
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString(
                "Settings index.remote_store.translog.repository can only be set/enabled when index.remote_store.translog.enabled is set to true"
            )
        );
    }

    public void testRemoteStoreOverrideOnlyTranslogRepoWithRemoteStoreEnabledIllegalArgumentException() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SETTING_REMOTE_STORE_ENABLED, true)
            .put(SETTING_REMOTE_STORE_REPOSITORY, "my-custom-repo")
            .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "my-custom-repo")
            .build();
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString(
                "Settings index.remote_store.translog.repository can only be set/enabled when index.remote_store.translog.enabled is set to true"
            )
        );
    }

    public void testRemoteStoreOverrideTranslogRepoCorrectly() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SETTING_REMOTE_STORE_ENABLED, true)
            .put(SETTING_REMOTE_STORE_REPOSITORY, "my-custom-repo")
            .put(SETTING_REMOTE_TRANSLOG_STORE_ENABLED, true)
            .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "my-custom-repo")
            .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices("test-idx-1").includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get("test-idx-1");
        verifyRemoteStoreIndexSettings(
            indexSettings,
            "true",
            "my-custom-repo",
            "true",
            "my-custom-repo",
            ReplicationType.SEGMENT.toString(),
            null
        );
    }

    public void testRemoteStoreOverrideReplicationTypeIndexSettings() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
            .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices("test-idx-1").includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get("test-idx-1");
        verifyRemoteStoreIndexSettings(indexSettings, null, null, null, null, ReplicationType.DOCUMENT.toString(), null);
    }

    protected void verifyRemoteStoreIndexSettings(
        Settings indexSettings,
        String isRemoteSegmentEnabled,
        String remoteSegmentRepo,
        String isRemoteTranslogEnabled,
        String remoteTranslogRepo,
        String replicationType,
        String translogBufferInterval
    ) {
        assertEquals(replicationType, indexSettings.get(SETTING_REPLICATION_TYPE));
        assertEquals(isRemoteSegmentEnabled, indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertEquals(remoteSegmentRepo, indexSettings.get(SETTING_REMOTE_STORE_REPOSITORY));
        assertEquals(isRemoteTranslogEnabled, indexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_ENABLED));
        assertEquals(remoteTranslogRepo, indexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
        assertEquals(translogBufferInterval, indexSettings.get(SETTING_REMOTE_TRANSLOG_BUFFER_INTERVAL));
    }

}
