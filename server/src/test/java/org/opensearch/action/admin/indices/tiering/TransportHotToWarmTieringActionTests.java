/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.MockInternalClusterInfoService;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class TransportHotToWarmTieringActionTests extends OpenSearchIntegTestCase {
    protected static final String TEST_IDX_1 = "test-idx-1";
    protected static final String TEST_IDX_2 = "idx-2";
    protected static final String TARGET_TIER = "warm";
    private String[] indices;

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.TIERED_REMOTE_INDEX, true);
        return featureSettings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockInternalClusterInfoService.TestPlugin.class);
    }

    @Before
    public void setup() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().ensureAtLeastNumSearchAndDataNodes(1);
        long bytes = new ByteSizeValue(1000, ByteSizeUnit.KB).getBytes();
        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, bytes, bytes - 1));

        final int numReplicasIndex = 0;
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicasIndex)
            .build();

        indices = new String[] { TEST_IDX_1, TEST_IDX_2 };
        for (String index : indices) {
            assertAcked(client().admin().indices().prepareCreate(index).setSettings(settings).get());
            ensureGreen(index);
        }
    }

    @After
    public void cleanup() {
        client().admin().indices().prepareDelete(indices).get();
    }

    MockInternalClusterInfoService getMockInternalClusterInfoService() {
        return (MockInternalClusterInfoService) internalCluster().getCurrentClusterManagerNodeInstance(ClusterInfoService.class);
    }

    static FsInfo.Path setDiskUsage(FsInfo.Path original, long totalBytes, long freeBytes) {
        return new FsInfo.Path(original.getPath(), original.getMount(), totalBytes, freeBytes, freeBytes);
    }

    public void testIndexLevelBlocks() {
        enableIndexBlock(TEST_IDX_1, SETTING_READ_ONLY_ALLOW_DELETE);
        TieringIndexRequest request = new TieringIndexRequest(TARGET_TIER, TEST_IDX_1);
        expectThrows(ClusterBlockException.class, () -> client().execute(HotToWarmTieringAction.INSTANCE, request).actionGet());
    }

    public void testIndexNotFound() {
        TieringIndexRequest request = new TieringIndexRequest(TARGET_TIER, "foo");
        expectThrows(IndexNotFoundException.class, () -> client().execute(HotToWarmTieringAction.INSTANCE, request).actionGet());
    }

    public void testNoConcreteIndices() {
        TieringIndexRequest request = new TieringIndexRequest(TARGET_TIER, "foo");
        request.indicesOptions(IndicesOptions.fromOptions(true, true, true, false));
        HotToWarmTieringResponse response = client().admin().indices().execute(HotToWarmTieringAction.INSTANCE, request).actionGet();
        assertTrue(response.isAcknowledged());
        assertTrue(response.getFailedIndices().isEmpty());
    }

    public void testNoAcceptedIndices() {
        TieringIndexRequest request = new TieringIndexRequest(TARGET_TIER, "test-idx-*", "idx-*");
        HotToWarmTieringResponse response = client().admin().indices().execute(HotToWarmTieringAction.INSTANCE, request).actionGet();
        assertTrue(response.isAcknowledged());
        assertEquals(2, response.getFailedIndices().size());
        for (HotToWarmTieringResponse.IndexResult result : response.getFailedIndices()) {
            assertEquals("index is not backed up by the remote store", result.getFailureReason());
        }
    }
}
