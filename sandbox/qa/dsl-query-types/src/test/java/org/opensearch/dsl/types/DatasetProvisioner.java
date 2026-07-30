/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.types;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
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
 * Provisions a {@link Dataset} into a parquet/composite index over REST: reads {@code mapping.json}
 * and {@code bulk.json} from the dataset's resource dir, splices in the parquet data-format settings,
 * creates the index, and bulk-ingests the docs.
 *
 * <p>The injected settings are the canonical form required by the DSL/analytics engine:
 * <pre>
 *   "index.pluggable.dataformat.enabled": true,
 *   "index.pluggable.dataformat": "composite",
 *   "index.composite.primary_data_format": "parquet",
 *   "index.composite.secondary_data_formats": "lucene"
 * </pre>
 * so this is the single place index-creation settings live for the whole module.
 *
 * <p>Local copy tailored to this standalone module (no dependency on analytics-engine-rest).
 */
public final class DatasetProvisioner {

    private static final Logger logger = LogManager.getLogger(DatasetProvisioner.class);

    private DatasetProvisioner() {}

    /** Provision the dataset into its parquet index. Throws (propagating the HTTP error) if the
     * backend rejects the mapping — the expected path for geo/nested field shapes. */
    public static void provision(RestClient client, Dataset dataset) throws IOException {
        // Delete if it already exists — ignore "not found".
        try {
            client.performRequest(new Request("DELETE", "/" + dataset.indexName));
        } catch (ResponseException e) {
            // index may not exist — ignore
        }

        String mapping = loadResource(dataset.mappingResourcePath());
        Request createIndex = new Request("PUT", "/" + dataset.indexName);
        createIndex.setJsonEntity(injectParquetSettings(mapping)); // may throw ResponseException (e.g. geo/nested → HTTP 400)
        client.performRequest(createIndex);

        String bulk = loadResource(dataset.bulkResourcePath());
        Request bulkRequest = new Request("POST", "/" + dataset.indexName + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.setOptions(bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Response bulkResponse = client.performRequest(bulkRequest);
        assertEquals("bulk ingest failed for " + dataset.indexName, 200, bulkResponse.getStatusLine().getStatusCode());
        // The _bulk API returns HTTP 200 even when individual items fail (e.g. a parquet/composite
        // index sets index.append_only.enabled, which rejects custom document ids). Fail loudly on
        // per-item errors so a silent zero-ingest can't masquerade as a successful provision.
        String bulkBody = new String(bulkResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        if (bulkBody.contains("\"errors\":true")) {
            throw new IOException("bulk ingest reported item errors for " + dataset.indexName + ": " + bulkBody);
        }

        Request flush = new Request("POST", "/" + dataset.indexName + "/_flush");
        flush.addParameter("force", "true");
        client.performRequest(flush);

        // Wait for the primary to be active before the first query. YELLOW (not GREEN): on a single-node
        // dev server any replica stays unassigned forever, which would time out a green wait.
        Request health = new Request("GET", "/_cluster/health/" + dataset.indexName);
        health.addParameter("wait_for_status", "yellow");
        health.addParameter("wait_for_no_initializing_shards", "true");
        health.addParameter("timeout", "60s");
        client.performRequest(health);

        logger.info("Dataset [{}] provisioned into parquet index [{}]", dataset.name, dataset.indexName);
    }

    /**
     * Splice the parquet/composite data-format settings into the mapping body's settings block,
     * anchored on the literal {@code "number_of_shards"} token. {@code secondary_data_formats} is the
     * single string {@code "lucene"} so text-search predicates keep a Lucene backend.
     */
    private static String injectParquetSettings(String mappingBody) {
        return mappingBody.replace(
            "\"number_of_shards\"",
            "\"index.pluggable.dataformat.enabled\": true, "
                + "\"index.pluggable.dataformat\": \"composite\", "
                + "\"index.composite.primary_data_format\": \"parquet\", "
                + "\"index.composite.secondary_data_formats\": \"lucene\", "
                + "\"number_of_shards\""
        );
    }

    /** Load a classpath resource as a UTF-8 string (trailing newline preserved for ndjson). */
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
