/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
// Uncomment the below line to enable trace level logs for this test for better debugging
// @TestLogging(reason = "Getting trace logs from composite directory package", value = "org.opensearch.index.store:TRACE")
public class CompositeDirectoryIT extends RemoteStoreBaseIntegTestCase {

    /*
    Disabling MockFSIndexStore plugin as the MockFSDirectoryFactory wraps the FSDirectory over a OpenSearchMockDirectoryWrapper which extends FilterDirectory (whereas FSDirectory extends BaseDirectory)
    As a result of this wrapping the local directory of Composite Directory does not satisfy the assertion that local directory must be of type FSDirectory
     */
    @Override
    protected boolean addMockIndexStorePlugin() {
        return false;
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.WRITEABLE_REMOTE_INDEX, true);

        return featureSettings.build();
    }

    public void testCompositeDirectory() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey(), "partial")
            .build();
        assertAcked(client().admin().indices().prepareCreate("test-idx-1").setSettings(settings).get());

        // Check if the Directory initialized for the IndexShard is of Composite Directory type
        IndexService indexService = internalCluster().getDataNodeInstance(IndicesService.class).indexService(resolveIndex("test-idx-1"));
        IndexShard shard = indexService.getShardOrNull(0);
        Directory directory = (((FilterDirectory) (((FilterDirectory) (shard.store().directory())).getDelegate())).getDelegate());
        assertTrue(directory instanceof CompositeDirectory);

        // Verify from the cluster settings if the data locality is partial
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices("test-idx-1").includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get("test-idx-1");
        assertEquals("partial", indexSettings.get("index.store.data_locality"));

        // Index data and ensure cluster does not turn red while indexing
        Map<String, Long> stats = indexData(10, false, "test-idx-1");
        refresh("test-idx-1");
        ensureGreen("test-idx-1");

        // Search and verify that the total docs indexed match the search hits
        SearchResponse searchResponse3 = client().prepareSearch("test-idx-1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertHitCount(searchResponse3, stats.get(TOTAL_OPERATIONS));
    }
}
