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
 * Integration tests for {@code GET /_plugins/lucene/{index}/_stats} and
 * {@code GET /_plugins/lucene/_nodes/{nodeId}/_stats}.
 *
 * @opensearch.experimental
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class LuceneStatsIT extends AbstractCompositeEngineIT {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public void testLuceneIndexStats() throws Exception {
        String idx = "lucene-stats-1";
        createCompositeIndex(idx, true);
        indexDocs(idx, 50, 0);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/lucene/" + idx + "/_stats"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        Map<String, Object> body = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);

        Map<String, Object> indexStats = (Map<String, Object>) ((Map<String, Object>) body.get("indices")).get(idx);
        Map<String, Object> indexing = (Map<String, Object>) indexStats.get("indexing");
        assertThat(indexing, notNullValue());
        assertThat(((Number) indexing.get("docs_indexed_total")).longValue(), equalTo(50L));
    }

    @SuppressWarnings("unchecked")
    public void testLuceneNodeStats() throws Exception {
        String idx = "lucene-node-stats";
        createCompositeIndex(idx, true);
        indexDocs(idx, 25, 0);
        flushIndex(idx);

        Response response = getRestClient().performRequest(new Request("GET", "/_plugins/lucene/_nodes/_stats"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        Map<String, Object> body = XContentHelper.convertToMap(JsonXContent.jsonXContent, response.getEntity().getContent(), true);
        assertThat(body, hasKey("nodes"));
        assertThat(((Map<?, ?>) body.get("nodes")).size(), greaterThanOrEqualTo(1));
    }

    public void testUnknownFormatReturns404() {
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/no-such-format/myindex/_stats"));
            fail("expected error for unknown format");
        } catch (Exception e) {
            assertThat(e, notNullValue());
        }
    }
}
