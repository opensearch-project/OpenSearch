/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.Map;

/**
 * End-to-end integration test for pure Parquet indexing with DataFusion.
 * <p>
 * Validates that a composite index with parquet as primary data format can be
 * created, documents can be ingested, and the index settings are correctly persisted.
 * <p>
 * Requires plugins: analytics-engine, analytics-backend-datafusion, analytics-backend-lucene,
 * dsl-query-executor, composite-engine, parquet-data-format.
 * <p>
 * Requires feature flag: {@code opensearch.experimental.feature.pluggable.dataformat.enabled=true}
 */
public class ParquetDataFusionIT extends DataFusionRestTestCase {

    private static final String INDEX_NAME = "parquet_e2e_test";

    /**
     * Creates a parquet-format index, verifies settings are persisted correctly,
     * ingests documents, and runs a simple search to confirm the index is functional.
     */
    public void testParquetIndexCreationAndIngestion() throws Exception {
        // Clean up if exists from a previous run
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (Exception e) {
            // index may not exist
        }

        // Create index with parquet as primary data format
        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"name\": { \"type\": \"keyword\" },"
            + "    \"age\": { \"type\": \"integer\" },"
            + "    \"score\": { \"type\": \"double\" },"
            + "    \"city\": { \"type\": \"keyword\" }"
            + "  }"
            + "}"
            + "}";

        Request createIndex = new Request("PUT", "/" + INDEX_NAME);
        createIndex.setJsonEntity(body);
        Map<String, Object> createResponse = assertOkAndParse(client().performRequest(createIndex), "Create parquet index");
        assertEquals("Index creation should be acknowledged", true, createResponse.get("acknowledged"));
        logger.info("Created parquet index [{}]", INDEX_NAME);

        // Wait for green health
        Request healthRequest = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        healthRequest.addParameter("wait_for_status", "green");
        healthRequest.addParameter("timeout", "30s");
        client().performRequest(healthRequest);

        // Verify index settings
        Response settingsResponse = client().performRequest(new Request("GET", "/" + INDEX_NAME + "/_settings"));
        Map<String, Object> settingsMap = assertOkAndParse(settingsResponse, "Get index settings");

        @SuppressWarnings("unchecked")
        Map<String, Object> indexSettings = (Map<String, Object>) settingsMap.get(INDEX_NAME);
        assertNotNull("Settings response should contain index", indexSettings);

        @SuppressWarnings("unchecked")
        Map<String, Object> settings = (Map<String, Object>) indexSettings.get("settings");
        @SuppressWarnings("unchecked")
        Map<String, Object> index = (Map<String, Object>) settings.get("index");
        @SuppressWarnings("unchecked")
        Map<String, Object> composite = (Map<String, Object>) index.get("composite");

        assertEquals("Primary data format should be parquet", "parquet", composite.get("primary_data_format"));
        logger.info("Verified index settings: primary_data_format = parquet");

        // Bulk index 5 documents
        StringBuilder bulk = new StringBuilder();
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"name\": \"alice\", \"age\": 30, \"score\": 95.5, \"city\": \"seattle\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"name\": \"bob\", \"age\": 25, \"score\": 88.0, \"city\": \"portland\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"name\": \"carol\", \"age\": 35, \"score\": 92.3, \"city\": \"seattle\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"name\": \"dave\", \"age\": 28, \"score\": 76.8, \"city\": \"portland\"}\n");
        bulk.append("{\"index\": {}}\n");
        bulk.append("{\"name\": \"eve\", \"age\": 32, \"score\": 91.0, \"city\": \"seattle\"}\n");

        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulk.toString());
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(
            bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build()
        );
        Map<String, Object> bulkResponse = assertOkAndParse(client().performRequest(bulkRequest), "Bulk index");
        assertEquals("Bulk indexing should have no errors", false, bulkResponse.get("errors"));
        logger.info("Indexed 5 documents into parquet index [{}]", INDEX_NAME);

        // Simple search to verify index is functional
        Request searchRequest = new Request("POST", "/" + INDEX_NAME + "/_search");
        searchRequest.setJsonEntity("{\"size\": 0, \"track_total_hits\": true}");
        Response searchResponse = client().performRequest(searchRequest);
        Map<String, Object> searchMap = assertOkAndParse(searchResponse, "Simple search");
        assertNotNull("Search response should contain hits", searchMap.get("hits"));
        logger.info("Simple search completed successfully on parquet index [{}]", INDEX_NAME);
    }
}
