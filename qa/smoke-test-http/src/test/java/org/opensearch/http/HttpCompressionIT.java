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

package org.opensearch.http;

import org.apache.hc.client5.http.entity.GzipDecompressingEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

public class HttpCompressionIT extends OpenSearchRestTestCase {

    private static final String GZIP_ENCODING = "gzip";
    private static final String SAMPLE_DOCUMENT = """
        {
           "name": {
              "first name": "Steve",
              "last name": "Jobs"
           }
        }
        """;

    public void testUncompressesResponseIfRequested() throws IOException, ParseException {
        // See please https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Accept-Encoding
        RequestOptions requestOptions = RequestOptions.DEFAULT.toBuilder()
                .addHeader(HttpHeaders.ACCEPT_ENCODING, "identity")
                .build();

        Request request = new Request("POST", "/company/_doc/2");
        request.setOptions(requestOptions);
        request.setJsonEntity(SAMPLE_DOCUMENT);
        
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());
        assertNull(response.getHeader(HttpHeaders.CONTENT_ENCODING));
        assertThat(response.getEntity(), is(not(instanceOf(GzipDecompressingEntity.class))));

        request = new Request("GET", "/company/_doc/2");
        response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(GZIP_ENCODING, response.getHeader(HttpHeaders.CONTENT_ENCODING));
        assertThat(response.getEntity(), instanceOf(ByteArrayEntity.class));

        String body = EntityUtils.toString(response.getEntity());
        assertThat(body, containsString(SAMPLE_DOCUMENT));
    }

    public void testCompressedResponseByDefault() throws IOException {
        Response response = client().performRequest(new Request("GET", "/"));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(GZIP_ENCODING, response.getHeader(HttpHeaders.CONTENT_ENCODING));

        Request request = new Request("POST", "/company/_doc/1");
        request.setJsonEntity(SAMPLE_DOCUMENT);
        response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());
        assertEquals(GZIP_ENCODING, response.getHeader(HttpHeaders.CONTENT_ENCODING));
        assertThat(response.getEntity(), is(not(instanceOf(GzipDecompressingEntity.class))));
    }

}
