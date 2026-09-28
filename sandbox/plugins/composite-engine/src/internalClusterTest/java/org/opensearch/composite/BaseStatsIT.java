/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.client.Response;

import java.io.IOException;
import java.util.Map;

/**
 * Abstract base class for stats API integration tests.
 * Provides helper methods for querying parquet and lucene stats endpoints
 * and asserting on JSON response fields.
 *
 * @opensearch.experimental
 */
public abstract class BaseStatsIT extends AbstractCompositeEngineIT {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    protected Map<String, Object> parquetIndexStats(String index, String... queryParams) throws IOException {
        return StatsITHelpers.parquetIndexStats(getRestClient(), index, queryParams);
    }

    protected Map<String, Object> luceneIndexStats(String index, String... queryParams) throws IOException {
        return StatsITHelpers.luceneIndexStats(getRestClient(), index, queryParams);
    }

    protected Map<String, Object> compositeIndexStats(String index, String... queryParams) throws IOException {
        return StatsITHelpers.compositeIndexStats(getRestClient(), index, queryParams);
    }

    protected Map<String, Object> compositeNodeStats(String nodeIdOrEmpty, String... queryParams) throws IOException {
        return StatsITHelpers.compositeNodeStats(getRestClient(), nodeIdOrEmpty, queryParams);
    }

    protected Map<String, Object> parquetNodeStats(String nodeIdOrEmpty, String... queryParams) throws IOException {
        return StatsITHelpers.parquetNodeStats(getRestClient(), nodeIdOrEmpty, queryParams);
    }

    protected Map<String, Object> luceneNodeStats(String nodeIdOrEmpty, String... queryParams) throws IOException {
        return StatsITHelpers.luceneNodeStats(getRestClient(), nodeIdOrEmpty, queryParams);
    }

    protected int getStatusCode(Map<String, Object> response) {
        return StatsITHelpers.getStatusCode(response);
    }

    protected long getCounter(Map<String, Object> response, String dottedPath) {
        return StatsITHelpers.getCounter(response, dottedPath);
    }

    protected boolean hasPath(Map<String, Object> response, String dottedPath) {
        return StatsITHelpers.hasPath(response, dottedPath);
    }

    protected void assertCounter(String message, Map<String, Object> response, String path, long expected) {
        StatsITHelpers.assertCounter(message, response, path, expected);
    }

    protected void assertCounterAtLeast(String message, Map<String, Object> response, String path, long min) {
        StatsITHelpers.assertCounterAtLeast(message, response, path, min);
    }

    protected void assertCounterPresent(String message, Map<String, Object> response, String path) {
        StatsITHelpers.assertCounterPresent(message, response, path);
    }

    protected void forceMerge(String index, int maxSegments) {
        StatsITHelpers.forceMerge(client(), index, maxSegments);
    }

    protected Response getRaw(String path) throws IOException {
        return StatsITHelpers.getRaw(getRestClient(), path);
    }
}
