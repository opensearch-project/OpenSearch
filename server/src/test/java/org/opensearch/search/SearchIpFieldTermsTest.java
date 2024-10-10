/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

public class SearchIpFieldTermsTest extends OpenSearchSingleNodeTestCase {

    static String defaultIndexName = "test";

    public void testOneAddr() throws Exception {
        XContentBuilder xcb = createMapping();
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(xcb).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setSource("{\"addr\": \"192.168.0.1\"}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        SearchResponse result = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.termsQuery("addr","192.168.0.1" ))
            .get();
        assertThat(result.getHits().getTotalHits().value, equalTo(1L));
    }

    private XContentBuilder createMapping() throws IOException {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
                .startObject("addr")
                    .field("type", "ip")
                    .startObject("fields")
                        .startObject("idx")
                            .field("type","ip")
                            .field("doc_values",false)
                        .endObject()
                        .startObject("dv")
                            .field("type","ip")
                            .field("index",false)
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()
            .endObject();
        return xcb;
    }
}
