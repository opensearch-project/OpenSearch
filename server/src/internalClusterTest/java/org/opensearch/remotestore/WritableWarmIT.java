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
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.shrink.ResizeType;
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
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.Node;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Assert;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.core.StringContains.containsString;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
// Uncomment the below line to enable trace level logs for this test for better debugging
// @TestLogging(reason = "Getting trace logs from composite directory package", value = "org.opensearch.index.store:TRACE")
public class WritableWarmIT extends RemoteStoreBaseIntegTestCase {

    protected static final String INDEX_NAME = "test-idx-1";
    protected static final String INDEX_NAME_2 = "test-idx-2";
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
        featureSettings.put(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, true);
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
        Settings clusterSettings = Settings.builder()
            .put(super.nodeSettings(0))
            .put(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, false)
            .build();

        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode(clusterSettings);
        internalTestCluster.startDataAndWarmNodes(1);

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), false)
            .build();

        try {
            prepareCreate(INDEX_NAME).setSettings(indexSettings).get();
            fail("Should have thrown Exception as setting should not be registered if Feature Flag is Disabled");
        } catch (SettingsException ex) {
            assertEquals(
                "unknown setting ["
                    + IndexModule.IS_WARM_INDEX_SETTING.getKey()
                    + "] please check that any required plugins are installed, or check the "
                    + "breaking changes documentation for removed settings",
                ex.getMessage()
            );
        }
    }

    public void testWritableWarmBasic() throws Exception {
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode();
        internalTestCluster.startDataAndWarmNodes(1);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
            .build();
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).get());

        // Verify from the cluster settings if the data locality is partial
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices(INDEX_NAME).includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get(INDEX_NAME);
        assertTrue(indexSettings.getAsBoolean(IndexModule.IS_WARM_INDEX_SETTING.getKey(), false));

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

    public void testFullFileAndFileCacheStats() throws ExecutionException, InterruptedException {

        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode();
        internalTestCluster.startDataAndWarmNodes(1);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME_2).setSettings(settings).get());

        // Ingesting docs again before force merge
        indexBulk(INDEX_NAME_2, NUM_DOCS_IN_BULK);
        flushAndRefresh(INDEX_NAME_2);

        // ensuring cluster is green
        ensureGreen();

        SearchResponse searchResponse = client().prepareSearch(INDEX_NAME_2).setQuery(QueryBuilders.matchAllQuery()).get();
        // Asserting that search returns same number of docs as ingested
        assertHitCount(searchResponse, NUM_DOCS_IN_BULK);

        // Ingesting docs again before force merge
        indexBulk(INDEX_NAME_2, NUM_DOCS_IN_BULK);
        flushAndRefresh(INDEX_NAME_2);

        FileCache fileCache = internalTestCluster.getDataNodeInstance(Node.class).fileCache();

        // TODO: Make these validation more robust, when SwitchableIndexInput is implemented.

        NodesStatsResponse nodesStatsResponse = client().admin().cluster().nodesStats(new NodesStatsRequest().all()).actionGet();

        AggregateFileCacheStats fileCacheStats = nodesStatsResponse.getNodes()
            .stream()
            .filter(n -> n.getNode().isDataNode())
            .toList()
            .getFirst()
            .getFileCacheStats();

        if (Objects.isNull(fileCacheStats)) {
            fail("File Cache Stats should not be null");
        }

        // Deleting the index (so that ref count drops to zero for all the files) and then pruning the cache to clear it to avoid any file
        // leaks
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME_2)).get());
        fileCache.prune();

        NodesStatsResponse response = client().admin().cluster().nodesStats(new NodesStatsRequest().all()).actionGet();
        int nonEmptyFileCacheNodes = 0;
        for (NodeStats stats : response.getNodes()) {
            AggregateFileCacheStats fcStats = stats.getFileCacheStats();
            if (Objects.isNull(fcStats) == false) {
                if (isFileCacheEmpty(fcStats) == false) {
                    nonEmptyFileCacheNodes++;
                }
            }
        }
        assertEquals(0, nonEmptyFileCacheNodes);

    }

    private boolean isFileCacheEmpty(AggregateFileCacheStats stats) {
        return stats.getUsed().getBytes() == 0L && stats.getActive().getBytes() == 0L;
    }

    public void testNoResizeOnWarm() {
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(1).get(0);
        Settings idxSettings = Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
            .build();

        createIndex(INDEX_NAME, idxSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        client().admin().indices().prepareUpdateSettings(INDEX_NAME).setSettings(Settings.builder().put("index.blocks.write", true)).get();
        ResizeType type = randomFrom(ResizeType.CLONE, ResizeType.SHRINK, ResizeType.SPLIT);
        try {
            client().admin()
                .indices()
                .prepareResizeIndex(INDEX_NAME, "target")
                .setResizeType(type)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", 2)
                        .putNull("index.blocks.write")
                        .build()
                )
                .get();
            fail();
        } catch (Exception e) {
            Assert.assertThat(e.getMessage(), containsString("cannot resize warm index"));
        }
    }
}
