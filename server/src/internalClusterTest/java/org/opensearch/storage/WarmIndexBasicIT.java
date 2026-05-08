/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
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
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.storage.directory.TieredDirectory;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Integration tests for basic warm index operations.
 *
 * @opensearch.experimental
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class WarmIndexBasicIT extends RemoteStoreBaseIntegTestCase {

    protected static final String INDEX_NAME = "test-idx-1";
    protected static final int NUM_DOCS_IN_BULK = 1000;

    @Override
    protected boolean addMockIndexStorePlugin() {
        return false;
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG, true);
        return featureSettings.build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ByteSizeValue cacheSize = new ByteSizeValue(1, ByteSizeUnit.GB);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .build();
    }

    public void testWritableWarm() throws Exception {
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode();
        internalTestCluster.startDataAndWarmNodes(1);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
            .put(IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), "tiered-storage")
            .build();
        // create a tiered-storage warm index with 1p0r configuration
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).get());

        // Verify from the cluster settings that the warm setting is true
        GetIndexResponse getIndexResponse = client().admin()
            .indices()
            .getIndex(new GetIndexRequest().indices(INDEX_NAME).includeDefaults(true))
            .get();
        Settings indexSettings = getIndexResponse.settings().get(INDEX_NAME);
        assertTrue(indexSettings.getAsBoolean(IndexModule.IS_WARM_INDEX_SETTING.getKey(), false));

        FileCache fileCache = internalTestCluster.getDataNodeInstance(Node.class).fileCache();
        IndexShard shard = internalTestCluster.getDataNodeInstance(IndicesService.class)
            .indexService(resolveIndex(INDEX_NAME))
            .getShardOrNull(0);
        Directory directory = unwrapToCompositeDirectory(shard.store().directory());

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
        CompositeDirectory compositeDir = (CompositeDirectory) directory;
        filesBeforeMerge.stream()
            .filter(file -> !FileTypeUtils.isLockFile(file))
            .filter(file -> !FileTypeUtils.isSegmentsFile(file))
            .forEach(file -> assertNull(fileCache.get(compositeDir.getFilePath(file))));

        // Deleting the index to avoid any file leaks
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());
    }

    public void testLocalDirectoryFilesAfterRefresh() throws Exception {
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode();
        internalTestCluster.startDataAndWarmNodes(1);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
            .put(IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), "tiered-storage")
            .build();
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).get());

        IndexShard shard = internalTestCluster.getDataNodeInstance(IndicesService.class)
            .indexService(resolveIndex(INDEX_NAME))
            .getShardOrNull(0);

        TieredDirectory tieredDirectory = unwrapToTieredDirectory(shard.store().directory());

        indexBulk(INDEX_NAME, NUM_DOCS_IN_BULK);
        refresh(INDEX_NAME);

        waitUntil(() -> {
            try {
                return Arrays.stream(tieredDirectory.listLocalFiles()).anyMatch(file -> file.contains("block"));
            } catch (IOException ignored) {
                return false;
            }
        }, 30, TimeUnit.SECONDS);
        assertTrue(
            Arrays.stream(tieredDirectory.listLocalFiles())
                .filter(file -> !file.contains("block"))
                .filter(file -> !file.contains("write.lock"))
                .findAny()
                .isEmpty()
        );

        // Deleting the index to avoid any file leaks
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());
    }

    /**
     * Unwraps the directory chain (walking through FilterDirectory wrappers including
     * BucketedCompositeDirectory) to find the underlying CompositeDirectory.
     */
    private static Directory unwrapToCompositeDirectory(Directory directory) {
        Directory current = directory;
        while (current instanceof FilterDirectory) {
            if (current instanceof CompositeDirectory) {
                return current;
            }
            current = ((FilterDirectory) current).getDelegate();
        }
        if (current instanceof CompositeDirectory) {
            return current;
        }
        throw new IllegalArgumentException("Expected CompositeDirectory but got: " + directory.getClass().getName());
    }

    /**
     * Unwraps the directory chain (walking through FilterDirectory wrappers including
     * BucketedCompositeDirectory) to find the underlying TieredDirectory.
     */
    private static TieredDirectory unwrapToTieredDirectory(Directory directory) {
        Directory current = directory;
        while (current instanceof FilterDirectory) {
            if (current instanceof TieredDirectory) {
                return (TieredDirectory) current;
            }
            current = ((FilterDirectory) current).getDelegate();
        }
        if (current instanceof TieredDirectory) {
            return (TieredDirectory) current;
        }
        throw new IllegalArgumentException("Expected TieredDirectory but got: " + directory.getClass().getName());
    }

    protected long getDocCount(String indexName) {
        refresh(indexName);
        SearchResponse response = client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setSize(0).get();
        return response.getHits().getTotalHits().value();
    }

    public void testCloseIndex() throws ExecutionException, InterruptedException {
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode();
        internalTestCluster.startDataAndWarmNodes(2);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
            .put(IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), "tiered-storage")
            .build();
        // create a warm index with 1p0r configuration
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).get());

        // Verify from the cluster settings if the warm index setting is true
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

        long docCount = getDocCount(INDEX_NAME);
        CloseIndexResponse closeIndexResponse = client().admin().indices().close(new CloseIndexRequest(INDEX_NAME)).get();
        assertTrue(closeIndexResponse.isShardsAcknowledged());

        OpenIndexResponse openIndexResponse = client().admin().indices().open(new OpenIndexRequest(INDEX_NAME)).get();
        assertTrue(openIndexResponse.isShardsAcknowledged());

        long docCountUpdated = getDocCount(INDEX_NAME);
        assertEquals(docCountUpdated, docCount);
    }

    public void testWritableWarmPrimaryReplicaBoth() throws Exception {
        InternalTestCluster internalTestCluster = internalCluster();
        internalTestCluster.startClusterManagerOnlyNode();
        internalTestCluster.startDataAndWarmNodes(2);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
            .put(IndexModule.INDEX_COMPOSITE_STORE_TYPE_SETTING.getKey(), "tiered-storage")
            .build();
        // create a tiered-storage warm index with 1p1r configuration
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).get());

        // Verify from the cluster settings if the warm index setting is true
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

        // Force merging the index
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).get();
        flushAndRefresh(INDEX_NAME);

        ensureGreen();
        searchResponse = client().prepareSearch(INDEX_NAME).setQuery(QueryBuilders.matchAllQuery()).get();
        // verify again after force merge search response return same no of docs as ingested
        assertHitCount(searchResponse, 2 * NUM_DOCS_IN_BULK);

        // Deleting the index to avoid any file leaks
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());
    }
}
