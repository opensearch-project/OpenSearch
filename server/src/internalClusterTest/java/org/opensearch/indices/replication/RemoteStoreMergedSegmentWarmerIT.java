/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.TieredMergePolicyProvider;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.nio.file.Path;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreMergedSegmentWarmerIT extends SegmentReplicationBaseIT {
    private Path absolutePath;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (absolutePath == null) {
            absolutePath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings("test-remote-store-repo", absolutePath))
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.MERGED_SEGMENT_WARMER_EXPERIMENTAL_FLAG, true);
        return featureSettings.build();
    }

    @Before
    public void setup() {
        internalCluster().startClusterManagerOnlyNode();
    }

    public void testMergeSegmentWarmerRemote() throws Exception {
        final String node1 = internalCluster().startDataOnlyNode();
        final String node2 = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo" + i, "bar" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        waitForSearchableDocs(30, node1, node2);

        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(2));
        waitForSegmentCount(INDEX_NAME, 2, logger);
    }

    public void testConcurrentMergeSegmentWarmerRemote() throws Exception {
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(indexSettings())
                .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), 5)
                .put(TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), 5)
                .put(IndexSettings.INDEX_MERGE_ON_FLUSH_ENABLED.getKey(), false)
                .build()
        );
        ensureGreen(INDEX_NAME);

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo" + i, "bar" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(2));

        waitForSegmentCount(INDEX_NAME, 2, logger);
    }

    public void testMergeSegmentWarmerWithInactiveReplicaRemote() throws Exception {
        internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);

        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo" + i, "bar" + i)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX_NAME).maxNumSegments(1)).get();
        final IndicesSegmentResponse response = client().admin().indices().prepareSegments(INDEX_NAME).get();
        assertEquals(1, response.getIndices().get(INDEX_NAME).getShards().values().size());
    }
}
