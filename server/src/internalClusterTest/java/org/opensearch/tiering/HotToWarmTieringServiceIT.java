/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tiering;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.tiering.HotToWarmTieringAction;
import org.opensearch.action.admin.indices.tiering.HotToWarmTieringResponse;
import org.opensearch.action.admin.indices.tiering.TieringIndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.MockInternalClusterInfoService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
// Uncomment the below line to enable trace level logs for this test for better debugging
// @TestLogging(reason = "Getting trace logs from tiering package", value =
// "org.opensearch.tiering:TRACE,org.opensearch.cluster.routing.allocation.decider:TRACE")
public class HotToWarmTieringServiceIT extends TieringBaseIntegTestCase {

    protected static final String TEST_IDX_1 = "test-idx-1";
    protected static final String TEST_IDX_2 = "test-idx-2";
    protected static final int NUM_DOCS_IN_BULK = 10;
    private static final long TOTAL_SPACE_BYTES = new ByteSizeValue(1000, ByteSizeUnit.KB).getBytes();

    @Before
    public void setup() {
        internalCluster().startClusterManagerOnlyNode();
    }

    // waiting for the recovery pr to be merged in
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/13647")
    public void testTieringBasic() {
        final int numReplicasIndex = 0;
        internalCluster().ensureAtLeastNumDataNodes(1);
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicasIndex)
            .put(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.FULL.name())
            .build();

        String[] indices = new String[] { TEST_IDX_1, TEST_IDX_2 };
        for (String index : indices) {
            assertAcked(client().admin().indices().prepareCreate(index).setSettings(settings).get());
            ensureGreen(index);
            // Ingesting some docs
            indexBulk(index, NUM_DOCS_IN_BULK);
            flushAndRefresh(index);
            ensureGreen();
            SearchResponse searchResponse = client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).get();
            // Asserting that search returns same number of docs as ingested
            assertHitCount(searchResponse, NUM_DOCS_IN_BULK);
        }

        // Spin up node having search role
        internalCluster().ensureAtLeastNumSearchAndDataNodes(1);

        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, TOTAL_SPACE_BYTES)
        );

        TieringIndexRequest request = new TieringIndexRequest(TARGET_WARM_TIER, indices);
        request.waitForCompletion(true);
        HotToWarmTieringResponse response = client().admin().indices().execute(HotToWarmTieringAction.INSTANCE, request).actionGet();
        assertAcked(response);
        assertTrue(response.getFailedIndices().isEmpty());
        assertTrue(response.isAcknowledged());
        ensureGreen();
        for (String index : indices) {
            SearchResponse searchResponse = client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).get();
            // Asserting that search returns same number of docs as ingested
            assertHitCount(searchResponse, NUM_DOCS_IN_BULK);
            GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().addIndices(index).get();
            assertWarmSettings(getIndexResponse, index);
            assertAcked(client().admin().indices().prepareDelete(index).get());
        }
    }

    private void assertWarmSettings(GetIndexResponse response, String indexName) {
        final Map<String, Settings> settings = response.settings();
        assertThat(settings, notNullValue());
        assertThat(settings.size(), equalTo(1));
        Settings indexSettings = settings.get(indexName);
        assertThat(indexSettings, notNullValue());
        assertThat(
            indexSettings.get(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey()),
            equalTo(IndexModule.DataLocalityType.PARTIAL.name())
        );
        assertThat(indexSettings.get(IndexModule.INDEX_TIERING_STATE.getKey()), equalTo(IndexModule.TieringState.WARM.name()));
    }
}
