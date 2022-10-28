/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.client;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.entity.GzipCompressingEntity;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.Matchers.equalTo;

public class HighLevelRestClientCompressionIT extends OpenSearchRestHighLevelClientTestCase {

    private static final String GZIP_ENCODING = "gzip";
    private static final String SAMPLE_DOCUMENT = "{\"name\":{\"first name\":\"Steve\",\"last name\":\"Jobs\"}}";

    public void testCompressesResponseIfRequested() throws IOException {
        Request doc = new Request(HttpPut.METHOD_NAME, "/company/_doc/1");
        doc.setJsonEntity(SAMPLE_DOCUMENT);
        client().performRequest(doc);
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));

        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder().addHeader(HttpHeaders.ACCEPT_ENCODING, GZIP_ENCODING).build();

        SearchRequest searchRequest = new SearchRequest("company");
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync, requestOptions);

        assertThat(searchResponse.status().getStatus(), equalTo(200));
        assertEquals(1L, searchResponse.getHits().getTotalHits().value);
        assertEquals(SAMPLE_DOCUMENT, searchResponse.getHits().getHits()[0].getSourceAsString());
    }

    /**
     * The default CloseableHttpAsyncClient does not support compression out of the box (so that applies to RestClient
     * and RestHighLevelClient). To check the compression works on both sides, crafting the request using CloseableHttpClient
     * instead which uses compression by default.
     */
    public void testCompressesRequest() throws IOException, URISyntaxException {
        try (CloseableHttpClient client = HttpClients.custom().build()) {
            final Node node = client().getNodes().iterator().next();
            final URI baseUri = new URI(node.getHost().toURI());

            final HttpPut index = new HttpPut(baseUri.resolve("/company/_doc/1"));
            index.setEntity(new GzipCompressingEntity(new StringEntity(SAMPLE_DOCUMENT, ContentType.APPLICATION_JSON)));
            try (CloseableHttpResponse response = client.execute(index)) {
                assertThat(response.getCode(), equalTo(201));
            }

            final HttpGet refresh = new HttpGet(baseUri.resolve("/_refresh"));
            try (CloseableHttpResponse response = client.execute(refresh)) {
                assertThat(response.getCode(), equalTo(200));
            }

            final HttpPost search = new HttpPost(baseUri.resolve("/_search"));
            index.setEntity(new GzipCompressingEntity(new StringEntity("{}", ContentType.APPLICATION_JSON)));
            try (CloseableHttpResponse response = client.execute(search)) {
                assertThat(response.getCode(), equalTo(200));
            }
        }
    }
}
