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
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class DataFusionRemoteStoreSimpleTests extends OpenSearchSingleNodeTestCase {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String INDEX_NAME = "datafusion-simple-index";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(DataFusionPlugin.class, ParquetDataFormatPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        Path repositoryPath = createTempDir();
        
        // Build remote store settings manually since remoteStoreClusterSettings is not available in SingleNodeTestCase
        String segmentRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            REPOSITORY_NAME
        );
        String segmentRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            REPOSITORY_NAME
        );
        
        return Settings.builder()
            .put(super.nodeSettings())
            .put("node.attr." + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, REPOSITORY_NAME)
            .put(segmentRepoTypeAttributeKey, "fs")
            .put(segmentRepoSettingsAttributeKeyPrefix + "location", repositoryPath)
            .put("node.attr." + REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, REPOSITORY_NAME)
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, REPOSITORY_NAME)
            .build();
    }

    @Override
    protected boolean resetNodeAfterTest() {
        // Don't reset node to avoid DataFusion cleanup issues
        return false;
    }

    protected Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }

    public void testBasicDataFusionWithRemoteStore() throws Exception {
        logger.info("--> TEST: Creating index with remote store and DataFusion");
        
        // Create index 
        assertAcked(client().admin().indices().prepareCreate(INDEX_NAME).setSettings(getIndexSettings()).get());
        ensureGreen(INDEX_NAME);
        logger.info("--> TEST: Index created and is green");

        // Index a simple document
        String testDoc = "{ \"name\": \"test1\", \"value\": 100, \"category\": \"A\" }";
        logger.info("--> TEST: Indexing document: {}", testDoc);
        
        try {
            client().prepareIndex(INDEX_NAME).setId("1").setSource(testDoc, MediaTypeRegistry.JSON).get();
            logger.info("--> TEST: Document indexed successfully");
        } catch (Exception e) {
            logger.error("--> TEST: Document indexing failed", e);
            throw e;
        }

        // Force refresh
        logger.info("--> TEST: Performing refresh");
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        logger.info("--> TEST: Refresh completed");

        // Verify document exists
        logger.info("--> TEST: Searching for documents");
        SearchResponse searchResponse = client().prepareSearch(INDEX_NAME).setSize(10).get();
        logger.info("--> TEST: Search completed, found {} documents", searchResponse.getHits().getTotalHits().value());
        
        assertEquals("Should have 1 document", 1L, searchResponse.getHits().getTotalHits().value());
        
        logger.info("--> TEST: Basic DataFusion with remote store test completed successfully");
    }
}
