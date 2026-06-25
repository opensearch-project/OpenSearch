/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Static utility methods shared across stats integration tests. Extracted from
 * {@link BaseStatsIT} so that test classes extending different base hierarchies
 * (e.g. {@link DataFormatAwareReplicationBaseIT}) can reuse the same helpers.
 *
 * @opensearch.experimental
 */
final class StatsITHelpers {

    private StatsITHelpers() {}

    // ---- REST helpers ----

    static Map<String, Object> parquetIndexStats(RestClient rest, String index, String... queryParams) throws IOException {
        return fetchStats(rest, "/_plugins/parquet/" + index + "/_stats", queryParams);
    }

    static Map<String, Object> luceneIndexStats(RestClient rest, String index, String... queryParams) throws IOException {
        return fetchStats(rest, "/_plugins/lucene/" + index + "/_stats", queryParams);
    }

    static Map<String, Object> compositeIndexStats(RestClient rest, String index, String... queryParams) throws IOException {
        return fetchStats(rest, "/_plugins/composite/" + index + "/_stats", queryParams);
    }

    static Map<String, Object> compositeNodeStats(RestClient rest, String nodeIdOrEmpty, String... queryParams) throws IOException {
        String path = nodeIdOrEmpty.isEmpty()
            ? "/_plugins/composite/_nodes/_stats"
            : "/_plugins/composite/_nodes/" + nodeIdOrEmpty + "/_stats";
        return fetchStats(rest, path, queryParams);
    }

    static Map<String, Object> parquetNodeStats(RestClient rest, String nodeIdOrEmpty, String... queryParams) throws IOException {
        String path = nodeIdOrEmpty.isEmpty() ? "/_plugins/parquet/_nodes/_stats" : "/_plugins/parquet/_nodes/" + nodeIdOrEmpty + "/_stats";
        return fetchStats(rest, path, queryParams);
    }

    static Map<String, Object> luceneNodeStats(RestClient rest, String nodeIdOrEmpty, String... queryParams) throws IOException {
        String path = nodeIdOrEmpty.isEmpty() ? "/_plugins/lucene/_nodes/_stats" : "/_plugins/lucene/_nodes/" + nodeIdOrEmpty + "/_stats";
        return fetchStats(rest, path, queryParams);
    }

    static Map<String, Object> fetchStats(RestClient rest, String path, String... queryParams) throws IOException {
        if (queryParams.length % 2 != 0) {
            throw new AssertionError("queryParams must be key-value pairs, got odd count: " + queryParams.length);
        }
        StringBuilder uri = new StringBuilder(path);
        for (int i = 0; i < queryParams.length; i += 2) {
            uri.append(i == 0 ? '?' : '&').append(queryParams[i]).append('=').append(queryParams[i + 1]);
        }
        Response response = rest.performRequest(new Request("GET", uri.toString()));
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
    }

    static Response getRaw(RestClient rest, String path) throws IOException {
        return rest.performRequest(new Request("GET", path));
    }

    // ---- Pure parsers ----

    static int getStatusCode(Map<String, Object> response) {
        Number status = (Number) response.get("status");
        return status != null ? status.intValue() : -1;
    }

    @SuppressWarnings("unchecked")
    static long getCounter(Map<String, Object> response, String dottedPath) {
        String[] segments = dottedPath.split("\\.");
        Object current = response;
        for (String segment : segments) {
            if (!(current instanceof Map)) return -1L;
            current = ((Map<String, Object>) current).get(segment);
            if (current == null) return -1L;
        }
        if (current instanceof Number) return ((Number) current).longValue();
        return -1L;
    }

    @SuppressWarnings("unchecked")
    static boolean hasPath(Map<String, Object> response, String dottedPath) {
        String[] segments = dottedPath.split("\\.");
        Object current = response;
        for (String segment : segments) {
            if (!(current instanceof Map)) return false;
            current = ((Map<String, Object>) current).get(segment);
            if (current == null) return false;
        }
        return true;
    }

    // ---- Assertion helpers ----

    static void assertCounter(String message, Map<String, Object> response, String path, long expected) {
        long actual = getCounter(response, path);
        assertEquals(message + " [path=" + path + "]", expected, actual);
    }

    static void assertCounterAtLeast(String message, Map<String, Object> response, String path, long min) {
        long actual = getCounter(response, path);
        assertTrue(message + " [path=" + path + ", actual=" + actual + ", min=" + min + "]", actual >= min);
    }

    static void assertCounterPresent(String message, Map<String, Object> response, String path) {
        long actual = getCounter(response, path);
        assertTrue(message + " [path=" + path + " must exist, got -1]", actual != -1L);
    }

    // ---- Action helpers ----

    static void forceMerge(org.opensearch.transport.client.Client client, String index, int maxSegments) {
        client.admin().indices().prepareForceMerge(index).setMaxNumSegments(maxSegments).get();
    }
}
