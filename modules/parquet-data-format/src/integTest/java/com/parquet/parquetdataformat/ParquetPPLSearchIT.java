/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat;

import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Integration tests for PPL search on replicated Parquet files.
 * Validates that Parquet files are searchable via PPL queries after replication.
 */
public class ParquetPPLSearchIT extends OpenSearchRestTestCase {

    private static final String INDEX_NAME = "parquet-ppl-search-test-idx";

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    /**
     * Tests that PPL search works after Parquet files are replicated.
     * Validates search results are correct, proving replication succeeded.
     */
    public void testSearchOnReplicatedParquetFiles() throws Exception {
        // Create index with segment replication
        Request createIndexRequest = new Request("PUT", "/" + INDEX_NAME);
        createIndexRequest.setJsonEntity("""
            {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                    "replication.type": "SEGMENT",
                    "refresh_interval": -1
                },
                "mappings": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "field": {"type": "text"},
                        "value": {"type": "long"}
                    }
                }
            }
            """);
        client().performRequest(createIndexRequest);

        // Index documents
        int numDocs = 20;
        long expectedSum = 0;
        StringBuilder bulkBody = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            long value = randomLongBetween(0, 1000);
            expectedSum += value;
            bulkBody.append(String.format(java.util.Locale.ROOT,
                "{\"index\":{\"_index\":\"%s\",\"_id\":\"%d\"}}\n", INDEX_NAME, i));
            bulkBody.append(String.format(java.util.Locale.ROOT,
                "{\"id\":\"%d\",\"field\":\"search_test_%d\",\"value\":%d}\n", i, i, value));
        }

        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulkBody.toString());
        bulkRequest.addParameter("refresh", "true");
        Response bulkResponse = client().performRequest(bulkRequest);
        assertEquals("Bulk indexing should succeed", 200, bulkResponse.getStatusLine().getStatusCode());
        String bulkResponseBody = new String(bulkResponse.getEntity().getContent().readAllBytes());
        logger.info("--> Bulk response: {}", bulkResponseBody);
        assertFalse("Bulk should not have errors: " + bulkResponseBody, bulkResponseBody.contains("\"errors\":true"));

        // Wait for green status (replication complete)
        Request healthRequest = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        healthRequest.addParameter("wait_for_status", "green");
        healthRequest.addParameter("timeout", "60s");
        client().performRequest(healthRequest);

        // Get node information to create node-specific clients
        Request nodesRequest = new Request("GET", "/_cat/nodes");
        nodesRequest.addParameter("format", "json");
        nodesRequest.addParameter("h", "ip,http,name");
        Response nodesResponse = client().performRequest(nodesRequest);
        String nodesJson = new String(nodesResponse.getEntity().getContent().readAllBytes());
        logger.info("--> Cluster nodes: {}", nodesJson);

        // Get shard allocation to identify primary and replica nodes
        Request catShardsRequest = new Request("GET", "/_cat/shards/" + INDEX_NAME);
        catShardsRequest.addParameter("format", "json");
        catShardsRequest.addParameter("h", "node,prirep");
        Response catShardsResponse = client().performRequest(catShardsRequest);
        String shardsJson = new String(catShardsResponse.getEntity().getContent().readAllBytes());
        logger.info("--> Shard allocation: {}", shardsJson);

        // Execute PPL count query on each node
        String countQuery = String.format("source=%s | stats count()", INDEX_NAME);

        // Get cluster hosts and filter to IPv4 only (avoid IPv6 duplicates)
        List<HttpHost> allHosts = getClusterHosts();
        List<HttpHost> hosts = allHosts.stream()
            .filter(h -> !h.getHostName().startsWith("["))
            .toList();
        logger.info("--> Available cluster hosts: {} (count: {})", hosts, hosts.size());

        if (hosts.size() >= 2) {
            // Direct node-specific queries
            for (int i = 0; i < 2; i++) {
                HttpHost host = hosts.get(i);
                try (RestClient nodeClient = buildClient(restClientSettings(), new HttpHost[]{host})) {
                    Request pplRequest = new Request("POST", "/_plugins/_ppl");
                    pplRequest.setJsonEntity("{\"query\": \"" + countQuery + "\"}");
                    String response = new String(nodeClient.performRequest(pplRequest).getEntity().getContent().readAllBytes());

                    logger.info("--> Node {} ({}) PPL response: {}", i, host, response);
                    String minified = response.replaceAll("\\s+", "");
                    assertTrue("Node " + i + " should return correct count", minified.contains("\"datarows\":[[" + numDocs + "]]"));
                }
            }
        } else {
            // Fallback: execute multiple times, load balancer will hit both nodes
            logger.info("--> Using load-balanced queries (hosts not directly accessible)");
            for (int attempt = 0; attempt < 10; attempt++) {
                String response = executePPLQuery(countQuery);
                String minified = response.replaceAll("\\s+", "");
                assertTrue("Attempt " + attempt + " should return correct count",
                    minified.contains("\"datarows\":[[" + numDocs + "]]"));
            }
        }

        // Execute PPL aggregation query
        String aggQuery = String.format("source=%s | stats sum(value) as total", INDEX_NAME);

        logger.info("--> Executing PPL aggregation queries (expected sum: {})", expectedSum);
        if (hosts.size() >= 2) {
            // Direct node-specific queries
            for (int i = 0; i < 2; i++) {
                HttpHost host = hosts.get(i);
                try (RestClient nodeClient = buildClient(restClientSettings(), new HttpHost[]{host})) {
                    Request pplRequest = new Request("POST", "/_plugins/_ppl");
                    pplRequest.setJsonEntity("{\"query\": \"" + aggQuery + "\"}");
                    String response = new String(nodeClient.performRequest(pplRequest).getEntity().getContent().readAllBytes());

                    logger.info("--> Node {} ({}) aggregation response: {}", i, host, response);
                    String minified = response.replaceAll("\\s+", "");
                    assertTrue("Node " + i + " should return correct sum", minified.contains("\"datarows\":[[" + expectedSum + "]]"));
                }
            }
        } else {
            // Fallback: execute multiple times
            for (int attempt = 0; attempt < 10; attempt++) {
                String response = executePPLQuery(aggQuery);
                String minified = response.replaceAll("\\s+", "");
                assertTrue("Attempt " + attempt + " should return correct sum",
                    minified.contains("\"datarows\":[[" + expectedSum + "]]"));
            }
        }
    }

    /**
     * Helper to execute PPL query via REST API (kept for potential future use).
     */
    private String executePPLQuery(String query) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + query + "\"}");
        Response response = client().performRequest(request);
        return new String(response.getEntity().getContent().readAllBytes());
    }
}
