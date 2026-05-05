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
    public static void provision(RestClient client, Dataset dataset) throws IOException {
        // Delete if exists
        try {
            client.performRequest(new Request("DELETE", "/" + dataset.indexName));
        } catch (Exception e) {
            // index may not exist — ignore
        }

        // Load mapping, inject parquet settings, create index
        String mapping = loadResource(dataset.mappingResourcePath());
        String indexBody = injectParquetSettings(mapping);
        Request createIndex = new Request("PUT", "/" + dataset.indexName);
        createIndex.setJsonEntity(indexBody);
        client.performRequest(createIndex);

        // Bulk ingest
        String bulkBody = loadResource(dataset.bulkResourcePath());
        Request bulkRequest = new Request("POST", "/" + dataset.indexName + "/_bulk");
        bulkRequest.setJsonEntity(bulkBody);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setOptions(
            bulkRequest.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build()
        );
        Response bulkResponse = client.performRequest(bulkRequest);
        assertEquals("Bulk insert failed", 200, bulkResponse.getStatusLine().getStatusCode());

        // Flush to commit parquet files to disk
        Request flushRequest = new Request("POST", "/" + dataset.indexName + "/_flush");
        flushRequest.addParameter("force", "true");
        client.performRequest(flushRequest);

        // Wait for index health
        Request healthRequest = new Request("GET", "/_cluster/health/" + dataset.indexName);
        healthRequest.addParameter("wait_for_status", "yellow");
        healthRequest.addParameter("timeout", "60s");
        client.performRequest(healthRequest);

        logger.info("Dataset [{}] provisioned into index [{}]", dataset.name, dataset.indexName);
    }

    /**
     * Inject parquet data format settings into the existing settings block.
     */
    private static String injectParquetSettings(String mappingBody) {
        return mappingBody.replace(
            "\"number_of_shards\"",
            "\"index.pluggable.dataformat.enabled\": true, "
                + "\"index.pluggable.dataformat\": \"composite\", "
                + "\"index.composite.primary_data_format\": \"parquet\", "
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
                return reader.lines().collect(Collectors.joining("\n"));
            }
        }
    }
}
