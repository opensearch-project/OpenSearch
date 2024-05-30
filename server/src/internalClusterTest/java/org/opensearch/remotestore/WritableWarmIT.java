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
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.index.store.remote.utils.cache.CacheUsage;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.Node;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
// Uncomment the below line to enable trace level logs for this test for better debugging
// @TestLogging(reason = "Getting trace logs from composite directory package", value = "org.opensearch.index.store:TRACE")
public class WritableWarmIT extends RemoteStoreBaseIntegTestCase {

    protected static final String INDEX_NAME = "test-idx-1";
    protected static final int NUM_DOCS_IN_BULK = 1000;

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
        featureSettings.put(FeatureFlags.TIERED_REMOTE_INDEX, true);
        return featureSettings.build();
    }

    public void testWritableWarmFeatureFlagDisabled() {
        Settings clusterSettings = Settings.builder().put(super.nodeSettings(0)).put(FeatureFlags.TIERED_REMOTE_INDEX, false).build();
        internalCluster().startDataOnlyNodes(1, clusterSettings);

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.PARTIAL.name())
            .build();

        assertThrows(
            "index.store.locality can be set to PARTIAL only if Feature Flag ["
                + FeatureFlags.TIERED_REMOTE_INDEX_SETTING.getKey()
                + "] is set to true",
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings).get()
        );
    }

    public void testWritableWarmBasic() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.PARTIAL.name())
            .build();
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).get());

        // Verify from the cluster settings if the data locality is partial
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices(INDEX_NAME).includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get(INDEX_NAME);
        assertEquals(IndexModule.DataLocalityType.PARTIAL.name(), indexSettings.get(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey()));

        // Ingesting some docs
        indexBulk(INDEX_NAME, NUM_DOCS_IN_BULK);
        flushAndRefresh(INDEX_NAME);

        // ensuring cluster is green after performing force-merge
        ensureGreen();

        SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).get();
        // Asserting that search returns same number of docs as ingested
        assertHitCount(searchResponse, NUM_DOCS_IN_BULK);

        // Ingesting docs again before force merge
        indexBulk(INDEX_NAME, NUM_DOCS_IN_BULK);
        flushAndRefresh(INDEX_NAME);

        FileCache fileCache = internalCluster().getDataNodeInstance(Node.class).fileCache();
        IndexShard shard = internalCluster().getDataNodeInstance(IndicesService.class)
            .indexService(resolveIndex(INDEX_NAME))
            .getShardOrNull(0);
        Directory directory = (((FilterDirectory) (((FilterDirectory) (shard.store().directory())).getDelegate())).getDelegate());

        // Force merging the index
        Set<String> filesBeforeMerge = new HashSet<>(Arrays.asList(directory.listAll()));
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        flushAndRefresh(INDEX_NAME);
        Set<String> filesAfterMerge = new HashSet<>(Arrays.asList(directory.listAll()));

        CacheUsage usageBeforePrune = fileCache.usage();
        fileCache.prune();
        CacheUsage usageAfterPrune = fileCache.usage();

        Set<String> filesFromPreviousGenStillPresent = filesBeforeMerge.stream()
            .filter(filesAfterMerge::contains)
            .filter(file -> !FileTypeUtils.isLockFile(file))
            .collect(Collectors.toUnmodifiableSet());

        // Asserting that after merge all the files from previous gen are no more part of the directory
        assertTrue(filesFromPreviousGenStillPresent.isEmpty());
        // Asserting that after the merge, refCount of some files in FileCache dropped to zero which resulted in their eviction after
        // pruning
        assertTrue(usageAfterPrune.usage() < usageBeforePrune.usage());

        // Clearing the file cache to avoid any file leaks
        fileCache.clear();
    }
}
