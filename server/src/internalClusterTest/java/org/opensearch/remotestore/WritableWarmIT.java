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
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.Node;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
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

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ByteSizeValue cacheSize = new ByteSizeValue(16, ByteSizeUnit.GB);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .build();
    }

    public void testWritableWarmFeatureFlagDisabled() {
        Settings clusterSettings = Settings.builder().put(super.nodeSettings(0)).put(FeatureFlags.TIERED_REMOTE_INDEX, false).build();
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode(clusterSettings);
        internalTestCluster.startDataAndSearchNodes(1);

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.PARTIAL.name())
            .build();

        try {
            prepareCreate(INDEX_NAME).setSettings(indexSettings).get();
            fail("Should have thrown Exception as setting should not be registered if Feature Flag is Disabled");
        } catch (SettingsException | IllegalArgumentException ex) {
            assertEquals(
                "unknown setting ["
                    + IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey()
                    + "] please check that any required plugins are installed, or check the "
                    + "breaking changes documentation for removed settings",
                ex.getMessage()
            );
        }
    }

    public void testWritableWarmBasic() throws Exception {
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode();
        internalTestCluster.startDataAndSearchNodes(1);
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

        FileCache fileCache = internalTestCluster.getDataNodeInstance(Node.class).fileCache();
        IndexShard shard = internalTestCluster.getDataNodeInstance(IndicesService.class)
            .indexService(resolveIndex(INDEX_NAME))
            .getShardOrNull(0);
        Directory directory = (((FilterDirectory) (((FilterDirectory) (shard.store().directory())).getDelegate())).getDelegate());

        // Force merging the index
        Set<String> filesBeforeMerge = new HashSet<>(Arrays.asList(directory.listAll()));
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        flushAndRefresh(INDEX_NAME);
        Set<String> filesAfterMerge = new HashSet<>(Arrays.asList(directory.listAll()));

        Set<String> filesFromPreviousGenStillPresent = filesBeforeMerge.stream()
            .filter(filesAfterMerge::contains)
            .filter(file -> !FileTypeUtils.isLockFile(file))
            .filter(file -> !FileTypeUtils.isSegmentsFile(file))
            .collect(Collectors.toUnmodifiableSet());

        // Asserting that after merge all the files from previous gen are no more part of the directory
        assertTrue(filesFromPreviousGenStillPresent.isEmpty());

        // Asserting that files from previous gen are not present in File Cache as well
        filesBeforeMerge.stream()
            .filter(file -> !FileTypeUtils.isLockFile(file))
            .filter(file -> !FileTypeUtils.isSegmentsFile(file))
            .forEach(file -> assertNull(fileCache.get(((CompositeDirectory) directory).getFilePath(file))));

        // Deleting the index (so that ref count drops to zero for all the files) and then pruning the cache to clear it to avoid any file
        // leaks
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());
        fileCache.prune();
    }
}
