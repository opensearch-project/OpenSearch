/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import com.parquet.parquetdataformat.ParquetDataFormatPlugin;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFusionRemoteStoreRecoveryTests extends OpenSearchIntegTestCase {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String INDEX_NAME = "datafusion-test-index";

    protected Path repositoryPath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataFusionPlugin.class, ParquetDataFormatPlugin.class);
    }

    @Before
    public void setup() {
        repositoryPath = randomRepoPath().toAbsolutePath();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, repositoryPath))
            .build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }

    public void testDataFusionWithRemoteStoreRecovery() throws Exception {
        logger.info("--> TEST: Starting DataFusion Remote Store Recovery test");
        
        // Step 1: Start cluster with remote store enabled
        logger.info("--> TEST: Starting cluster manager node");
        internalCluster().startClusterManagerOnlyNode();
        logger.info("--> TEST: Starting data node");
        internalCluster().startDataOnlyNode();
        logger.info("--> TEST: Ensuring stable cluster with 2 nodes");
        ensureStableCluster(2);
        logger.info("--> TEST: Cluster setup completed successfully");

        // Step 2: Create index with DataFusion settings
        logger.info("--> TEST: Creating index '{}' with settings: {}", INDEX_NAME, indexSettings());
        try {
            assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings()).get());
            logger.info("--> TEST: Index '{}' created successfully", INDEX_NAME);
        } catch (Exception e) {
            logger.error("--> TEST: Failed to create index '{}'", INDEX_NAME, e);
            throw e;
        }
        
        logger.info("--> TEST: Ensuring index '{}' is green", INDEX_NAME);
        ensureGreen(INDEX_NAME);
        logger.info("--> TEST: Index '{}' is green", INDEX_NAME);

        // Debug cluster state before indexing
        logger.info("--> TEST: Checking cluster state before indexing");
        logger.info("--> TEST: Cluster nodes: {}", client().admin().cluster().prepareState().get().getState().getNodes());
        logger.info("--> TEST: Index metadata: {}", client().admin().cluster().prepareState().get().getState().getMetadata().index(INDEX_NAME));
        
        // Test client connectivity
        logger.info("--> TEST: Testing client connectivity with cluster health check");
        try {
            var healthResponse = client().admin().cluster().prepareHealth(INDEX_NAME).get();
            logger.info("--> TEST: Cluster health for index '{}': status={}, active_shards={}", 
                INDEX_NAME, healthResponse.getStatus(), healthResponse.getActiveShards());
        } catch (Exception e) {
            logger.error("--> TEST: Cluster health check failed", e);
        }

        // Step 3: Index some test data with detailed logging
        logger.info("--> TEST: Starting indexing operations");
        String testDoc1 = "{ \"name\": \"test1\", \"value\": 100, \"category\": \"A\" }";
        String testDoc2 = "{ \"name\": \"test2\", \"value\": 200, \"category\": \"B\" }";
        String testDoc3 = "{ \"name\": \"test3\", \"value\": 150, \"category\": \"A\" }";

        // Index first document
        logger.info("--> TEST: Indexing document 1 with ID '1': {}", testDoc1);
        try {
            client().prepareIndex(INDEX_NAME).setId("1").setSource(testDoc1, MediaTypeRegistry.JSON).get();
            logger.info("--> TEST: Document 1 indexed successfully");
        } catch (Exception e) {
            logger.error("--> TEST: Failed to index document 1", e);
            throw e;
        }

        // Index second document
        logger.info("--> TEST: Indexing document 2 with ID '2': {}", testDoc2);
        try {
            client().prepareIndex(INDEX_NAME).setId("2").setSource(testDoc2, MediaTypeRegistry.JSON).get();
            logger.info("--> TEST: Document 2 indexed successfully");
        } catch (Exception e) {
            logger.error("--> TEST: Failed to index document 2", e);
            throw e;
        }

        // Index third document
        logger.info("--> TEST: Indexing document 3 with ID '3': {}", testDoc3);
        try {
            client().prepareIndex(INDEX_NAME).setId("3").setSource(testDoc3, MediaTypeRegistry.JSON).get();
            logger.info("--> TEST: Document 3 indexed successfully");
        } catch (Exception e) {
            logger.error("--> TEST: Failed to index document 3", e);
            throw e;
        }

        logger.info("--> TEST: All documents indexed successfully, proceeding with refresh and flush");

        // Force refresh and flush to ensure data is persisted to remote store
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Step 4: Verify initial data before recovery
        SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSize(10).get();
        assertEquals("Should have 3 documents before recovery", 3L, searchResponse.getHits().getTotalHits().value());

        // Step 5: Perform a simple query to verify DataFusion works before recovery
        SearchResponse queryResponse = client().prepareSearch(INDEX_NAME)
            .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("category", "A"))
            .get();
        assertEquals("Should find 2 documents with category A", 2L, queryResponse.getHits().getTotalHits().value());

        // Step 6: Restart cluster to trigger remote store recovery
        logger.info("--> Restarting cluster to test remote store recovery");
        internalCluster().fullRestart();
        ensureStableCluster(2);
        ensureGreen(INDEX_NAME);

        // Step 7: Verify data exists after recovery
        SearchResponse recoveredSearchResponse = client().prepareSearch(INDEX_NAME).setSize(10).get();
        assertEquals("Should have 3 documents after remote store recovery", 3L, recoveredSearchResponse.getHits().getTotalHits().value());

        // Step 8: Verify DataFusion queries work after recovery
        SearchResponse recoveredQueryResponse = client().prepareSearch(INDEX_NAME)
            .setQuery(org.opensearch.index.query.QueryBuilders.termQuery("category", "A"))
            .get();
        assertEquals("Should find 2 documents with category A after recovery", 2L, recoveredQueryResponse.getHits().getTotalHits().value());

        // Step 9: Verify aggregations work after recovery (test DataFusion functionality)
        SearchResponse aggResponse = client().prepareSearch(INDEX_NAME)
            .addAggregation(
                org.opensearch.search.aggregations.AggregationBuilders.terms("categories").field("category.keyword")
            )
            .get();

        assertNotNull("Aggregation should be present", aggResponse.getAggregations());

        logger.info("--> DataFusion remote store recovery test completed successfully");
    }

    public void testDataFusionQueryPerformanceAfterRecovery() throws Exception {
        // Step 1: Start cluster
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        // Step 2: Create index and add more test data for performance testing
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings()).get());
        ensureGreen(INDEX_NAME);

        // Index multiple documents for a more realistic test
        for (int i = 1; i <= 50; i++) {
            String doc = String.format(
                "{ \"id\": %d, \"value\": %d, \"category\": \"%s\", \"timestamp\": \"2023-01-%02d\" }",
                i, i * 10, (i % 3 == 0 ? "premium" : "standard"), (i % 28) + 1
            );
            client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource(doc, MediaTypeRegistry.JSON).get();
        }

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Step 3: Restart cluster
        logger.info("--> Restarting cluster for performance test");
        internalCluster().fullRestart();
        ensureStableCluster(2);
        ensureGreen(INDEX_NAME);

        // Step 4: Verify complex queries work after recovery
        SearchResponse complexQuery = client().prepareSearch(INDEX_NAME)
            .setQuery(
                org.opensearch.index.query.QueryBuilders.boolQuery()
                    .must(org.opensearch.index.query.QueryBuilders.rangeQuery("value").gte(100).lte(300))
                    .filter(org.opensearch.index.query.QueryBuilders.termQuery("category.keyword", "premium"))
            )
            .addSort("value", org.opensearch.search.sort.SortOrder.DESC)
            .setSize(5)
            .get();

        assertTrue("Should find premium items in value range", complexQuery.getHits().getTotalHits().value() > 0);

        logger.info("--> DataFusion performance test after recovery completed successfully");
    }
}
