/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

/**
 * Static helper that provisions the ClickBench test dataset into a live cluster
 * with parquet as the primary data format.
 * <p>
 * Stateless utility — all methods are static and take a {@link RestClient} parameter.
 * This makes it composable: any test class can use it without inheritance.
 * <p>
 * The ClickBench dataset uses the standard ClickBench schema (103 fields) with
 * 100 rows stored as bulk JSON for OpenSearch indexing.
 */
public final class ClickBenchTestFixture {

    private static final Logger logger = LogManager.getLogger(ClickBenchTestFixture.class);

    /** Index name used by all ClickBench tests. */
    public static final String INDEX_NAME = "parquet_hits";

    /** Expected document count after provisioning. */
    public static final int EXPECTED_DOC_COUNT = 100;

    private ClickBenchTestFixture() {
        // utility class
    }

    /**
     * Provision the ClickBench dataset into the cluster with parquet as the primary
     * data format. Idempotent: deletes the index first if it already exists, then
     * creates it with parquet settings, ClickBench mapping, and bulk-ingests 100 documents.
     *
     * @param client the REST client connected to the test cluster
     */
    public static void provisionIndex(RestClient client) throws IOException {
        // Delete if exists
        try {
            client.performRequest(new Request("DELETE", "/" + INDEX_NAME));
        } catch (Exception e) {
            // index may not exist — ignore
        }

        // Load the mapping from resource and inject parquet data format settings
        String mappingBody = loadResource("clickbench/parquet_hits_mapping.json");
        String indexBody = injectParquetSettings(mappingBody);

        Request createIndex = new Request("PUT", "/" + INDEX_NAME);
        createIndex.setJsonEntity(indexBody);
        client.performRequest(createIndex);

        // Bulk ingest
        String bulkBody = loadResource("clickbench/bulk.json");
        Request bulkRequest = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulkRequest.setJsonEntity(bulkBody);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(
            bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build()
        );
        Response bulkResponse = client.performRequest(bulkRequest);
        assertEquals("Bulk insert failed", 200, bulkResponse.getStatusLine().getStatusCode());

        // Flush to commit parquet files to disk
        Request flushRequest = new Request("POST", "/" + INDEX_NAME + "/_flush");
        flushRequest.addParameter("force", "true");
        client.performRequest(flushRequest);

        // Wait for index health
        Request healthRequest = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        healthRequest.addParameter("wait_for_status", "yellow");
        healthRequest.addParameter("timeout", "60s");
        client.performRequest(healthRequest);

        logger.info("Index [{}] provisioned with {} documents (parquet primary format)", INDEX_NAME, EXPECTED_DOC_COUNT);
    }

    /**
     * Inject parquet data format settings into the index creation body.
     * Adds pluggable dataformat and composite settings to the existing settings block.
     */
    private static String injectParquetSettings(String mappingBody) {
        // The mapping file has: {"settings": {"number_of_shards": 1, ...}, "mappings": {...}}
        // Inject parquet settings after the opening of the settings block
        return mappingBody.replace(
            "\"number_of_shards\"",
            "\"index.pluggable.dataformat.enabled\": true, "
                + "\"index.pluggable.dataformat\": \"composite\", "
                + "\"index.composite.primary_data_format\": \"parquet\", "
                + "\"number_of_shards\""
        );
    }

    /**
     * Load a DSL query JSON from {@code clickbench/dsl/q{N}.json}.
     */
    public static String loadDslQuery(int queryNumber) throws IOException {
        return loadResource("clickbench/dsl/q" + queryNumber + ".json");
    }

    /**
     * Load a PPL query from {@code clickbench/ppl/q{N}.ppl} (trimmed).
     */
    public static String loadPplQuery(int queryNumber) throws IOException {
        return loadResource("clickbench/ppl/q" + queryNumber + ".ppl").trim();
    }

    private static String loadResource(String path) throws IOException {
        try (InputStream is = ClickBenchTestFixture.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull("Resource not found: " + path, is);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        }
    }
}
