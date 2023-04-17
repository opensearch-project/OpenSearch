/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_STORE_ENABLED_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class CreateRemoteIndexTranslogDisabledIT extends CreateRemoteIndexIT {

    @Override
    protected Settings nodeSettings(int nodeOriginal) {
        Settings settings = super.nodeSettings(nodeOriginal);
        Settings.Builder builder = Settings.builder().put(settings);
        builder.remove(CLUSTER_REMOTE_TRANSLOG_STORE_ENABLED_SETTING.getKey());
        builder.remove(CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING.getKey());
        return builder.build();
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
        verifyRemoteStoreIndexSettings(indexSettings, "true", "my-custom-repo", null, null, ReplicationType.SEGMENT.toString(), null);
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
        verifyRemoteStoreIndexSettings(indexSettings, "true", "my-segment-repo-1", null, null, ReplicationType.SEGMENT.toString(), null);
    }

}
