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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Generic provisioner that creates an index from a {@link Dataset} descriptor.
 * <p>
 * Reads {@code mapping.json} and {@code bulk.json} from the dataset's resource
 * directory and ingests them into the cluster. Idempotent — deletes the index
 * first if it already exists.
 * <p>
 * Applies parquet data format settings so the dataset is queryable via the
 * DataFusion backend.
 */
public final class DatasetProvisioner {

    private static final Logger logger = LogManager.getLogger(DatasetProvisioner.class);

    private DatasetProvisioner() {
        // utility class
    }

    /**
     * Provision the dataset into the cluster with parquet as the primary data format.
     */
    public static void provision(RestClient client, Dataset dataset, int numberOfShards) throws IOException {
        for (String indexName : dataset.indexNames) {
            provisionIndex(client, dataset, indexName, numberOfShards);
        }
    }

    public static void provision(RestClient client, Dataset dataset) throws IOException {
        provision(client, dataset, 0);
    }

    /**
     * Provision the dataset with {@code numberOfShards} overriding the value in the mapping.
     * Pass {@code 0} to keep the mapping's value. Used by tests that need multi-shard
     * coverage of planner paths (exchange insertion, sort split, etc.).
     */
    private static void provisionIndex(RestClient client, Dataset dataset, String indexName, int numberOfShards) throws IOException {
        // Delete if exists
        try {
            client.performRequest(new Request("DELETE", "/" + indexName));
        } catch (Exception e) {
            // index may not exist — ignore
        }

        // Load mapping, inject parquet settings, create index
        String mappingPath = dataset.indexNames.size() == 1
            ? dataset.mappingResourcePath()
            : "datasets/" + dataset.name + "/mapping_" + indexName + ".json";
        String mapping = loadResource(mappingPath);
        String indexBody = injectParquetSettings(mapping);
        if (numberOfShards > 0) {
            indexBody = overrideNumberOfShards(indexBody, numberOfShards);
        }
        Request createIndex = new Request("PUT", "/" + indexName);
        createIndex.setJsonEntity(indexBody);
        client.performRequest(createIndex);

        // Bulk ingest
        String bulkPath = dataset.indexNames.size() == 1
            ? dataset.bulkResourcePath()
            : "datasets/" + dataset.name + "/bulk_" + indexName + ".json";
        String bulkBody = loadResource(bulkPath);
        Request bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(bulkBody);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(
            bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build()
        );
        Response bulkResponse = client.performRequest(bulkRequest);
        assertEquals("Bulk insert failed", 200, bulkResponse.getStatusLine().getStatusCode());

        // Log bulk response for debugging
        String responseBody = new String(bulkResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        logger.info("Bulk response for index [{}]: {}", indexName, responseBody);

        // Flush to commit parquet files to disk
        Request flushRequest = new Request("POST", "/" + indexName + "/_flush");
        flushRequest.addParameter("force", "true");
        client.performRequest(flushRequest);

        // Wait for index health. wait_for_status=yellow only guarantees primaries are assigned, not
        // that every shard copy is active and done initializing — on a multi-node cluster a search
        // can then race ahead of the shard becoming searchable and fail with "no such shard". Wait
        // for green + all shards active + none initializing so the first query always finds them.
        Request healthRequest = new Request("GET", "/_cluster/health/" + indexName);
        healthRequest.addParameter("wait_for_status", "green");
        healthRequest.addParameter("wait_for_active_shards", "all");
        healthRequest.addParameter("wait_for_no_initializing_shards", "true");
        healthRequest.addParameter("wait_for_no_relocating_shards", "true");
        healthRequest.addParameter("timeout", "60s");
        client.performRequest(healthRequest);

        logger.info("Dataset [{}] provisioned into index [{}]", dataset.name, indexName);
    }

    /**
     * Replace the {@code number_of_shards} value in the mapping body. Matches the form
     * {@code "number_of_shards": <int>} produced by the canonical dataset mappings.
     */
    private static String overrideNumberOfShards(String mappingBody, int numberOfShards) {
        return mappingBody.replaceAll("\"number_of_shards\"\\s*:\\s*\\d+", "\"number_of_shards\": " + numberOfShards);
    }

    /**
     * Inject parquet data format settings into the existing settings block.
     *
     * <p>Lucene is set as the secondary format so the Lucene analytics backend is available
     * for text-search functions (match, match_phrase, query_string, ...). Without it those
     * functions fail at planning time with
     * {@code "No backend can evaluate filter predicate [OTHER_FUNCTION] on fields [...:text]"}
     * because the Lucene backend never gets enrolled as a candidate.
     */
    private static String injectParquetSettings(String mappingBody) {
        return mappingBody.replace(
            "\"number_of_shards\"",
            "\"index.pluggable.dataformat.enabled\": true, "
                + "\"index.pluggable.dataformat\": \"composite\", "
                + "\"index.composite.primary_data_format\": \"parquet\", "
                + "\"index.composite.secondary_data_formats\": [\"lucene\"], "
                + "\"number_of_shards\""
        );
    }

    /**
     * Load a classpath resource as a UTF-8 string.
     */
    public static String loadResource(String path) throws IOException {
        try (InputStream is = DatasetProvisioner.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull("Resource not found: " + path, is);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String content = reader.lines().collect(Collectors.joining("\n"));
                return content.isEmpty() ? content : content + "\n";
            }
        }
    }
}
