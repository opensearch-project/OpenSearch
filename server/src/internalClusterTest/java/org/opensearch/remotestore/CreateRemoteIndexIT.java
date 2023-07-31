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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.index.IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING;
import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;
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
            .put(remoteStoreClusterSettings("my-segment-repo-1", "my-translog-repo-1"))
            .put(settings);
        return builder.build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.REMOTE_STORE, "true").build();
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
            "my-translog-repo-1",
            ReplicationType.SEGMENT.toString(),
            IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL
        );
    }

    public void testRemoteStoreDisabledByUser() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REMOTE_STORE_ENABLED, false)
            .build();

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString(
                String.format(
                    Locale.ROOT,
                    "Validation Failed: 1: private index setting [%s] can not be set explicitly;",
                    SETTING_REMOTE_STORE_ENABLED
                )
            )
        );
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
            containsString(
                String.format(
                    Locale.ROOT,
                    "Validation Failed: 1: private index setting [%s] can not be set explicitly;",
                    SETTING_REMOTE_STORE_ENABLED
                )
            )
        );
    }

    public void testReplicationTypeDocumentByUser() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
            .build();
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString(
                String.format(
                    Locale.ROOT,
                    "To enable %s, %s should be set to %s",
                    SETTING_REMOTE_STORE_ENABLED,
                    SETTING_REPLICATION_TYPE,
                    ReplicationType.SEGMENT
                )
            )
        );
    }

    public void testRemoteStoreSegmentRepoWithoutRemoteEnabledAndSegmentReplicationIllegalArgumentException() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "my-custom-repo")
            .build();
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString(
                String.format(
                    Locale.ROOT,
                    "Settings %s can only be set/enabled when %s is set to true",
                    SETTING_REMOTE_SEGMENT_STORE_REPOSITORY,
                    SETTING_REMOTE_STORE_ENABLED
                )
            )
        );
    }

    public void testRemoteStoreEnabledByUserWithRemoteRepoIllegalArgumentException() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SETTING_REMOTE_STORE_ENABLED, true)
            .put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "my-custom-repo")
            .build();

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString(
                String.format(
                    Locale.ROOT,
                    "Validation Failed: 1: private index setting [%s] can not be set explicitly;2: private index setting [%s] can not be set explicitly;",
                    SETTING_REMOTE_STORE_ENABLED,
                    SETTING_REMOTE_SEGMENT_STORE_REPOSITORY
                )
            )
        );
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
                String.format(
                    Locale.ROOT,
                    "Settings %s can only be set/enabled when %s is set to true",
                    SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY,
                    SETTING_REMOTE_STORE_ENABLED
                )
            )
        );
    }

    public void testRemoteStoreOverrideTranslogRepoCorrectly() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(SETTING_REMOTE_STORE_ENABLED, true)
            .put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "my-custom-repo")
            .put(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "my-custom-repo")
            .build();
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get()
        );
        assertThat(
            exc.getMessage(),
            containsString(
                String.format(
                    Locale.ROOT,
                    "Validation Failed: 1: private index setting [%s] can not be set explicitly;2: private index setting [%s] can not be set explicitly;3: private index setting [%s] can not be set explicitly;",
                    SETTING_REMOTE_STORE_ENABLED,
                    SETTING_REMOTE_SEGMENT_STORE_REPOSITORY,
                    SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY
                )
            )
        );
    }

    protected void verifyRemoteStoreIndexSettings(
        Settings indexSettings,
        String isRemoteSegmentEnabled,
        String remoteSegmentRepo,
        String remoteTranslogRepo,
        String replicationType,
        TimeValue translogBufferInterval
    ) {
        assertEquals(replicationType, indexSettings.get(SETTING_REPLICATION_TYPE));
        assertEquals(isRemoteSegmentEnabled, indexSettings.get(SETTING_REMOTE_STORE_ENABLED));
        assertEquals(remoteSegmentRepo, indexSettings.get(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertEquals(remoteTranslogRepo, indexSettings.get(SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
        assertEquals(translogBufferInterval, INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(indexSettings));
    }

}
