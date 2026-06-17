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
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for {@code GET /_plugins/parquet/{index}/_stats} and
 * {@code GET /_plugins/parquet/_nodes/{nodeId}/_stats}.
 *
 * @opensearch.experimental
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class ParquetStatsIT extends AbstractCompositeEngineIT {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // need real Netty4 HTTP transport for the REST client
    }

    @SuppressWarnings("unchecked")
    public void testParquetIndexStats() throws Exception {
        String idx = "parquet-stats-1";
        createCompositeIndex(idx, true);
        indexDocs(idx, 50, 0);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/parquet/" + idx + "/_stats"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        Map<String, Object> body = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);

        assertThat(body, hasKey("indices"));
        Map<String, Object> indices = (Map<String, Object>) body.get("indices");
        assertThat(indices, hasKey(idx));
        Map<String, Object> indexStats = (Map<String, Object>) indices.get(idx);
        Map<String, Object> indexing = (Map<String, Object>) indexStats.get("indexing");
        assertThat(indexing, notNullValue());
        assertThat(((Number) indexing.get("docs_indexed_total")).longValue(), equalTo(50L));
    }

    @SuppressWarnings("unchecked")
    public void testParquetIndexStatsShardLevel() throws Exception {
        String idx = "parquet-stats-shards";
        createCompositeIndex(idx, true);
        indexDocs(idx, 30, 0);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/parquet/" + idx + "/_stats?level=shards"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        Map<String, Object> body = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        Map<String, Object> indexStats = (Map<String, Object>) ((Map<String, Object>) body.get("indices")).get(idx);
        assertThat(indexStats, hasKey("shards"));
    }

    public void testParquetIndexStatsNonExistent() {
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/parquet/no-such-index/_stats"));
            fail("expected 404");
        } catch (Exception e) {
            assertThat(e, notNullValue());
        }
    }

    public void testParquetInvalidShard() throws Exception {
        String idx = "parquet-invalid-shard";
        createCompositeIndex(idx, true);
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/parquet/" + idx + "/_stats?shard=99"));
            fail("expected 400");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }

    @SuppressWarnings("unchecked")
    public void testParquetNodeStats() throws Exception {
        String idx = "parquet-node-stats";
        createCompositeIndex(idx, true);
        indexDocs(idx, 25, 0);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/parquet/_nodes/_stats"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        Map<String, Object> body = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);

        assertThat(body, hasKey("nodes"));
        Map<String, Object> nodes = (Map<String, Object>) body.get("nodes");
        assertThat("at least one node returns parquet stats", nodes.size(), greaterThanOrEqualTo(1));
    }
}
