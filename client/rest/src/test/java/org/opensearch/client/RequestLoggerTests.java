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

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpOptions;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.classic.methods.HttpTrace;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.ProtocolVersion;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.message.StatusLine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RequestLoggerTests extends RestClientTestCase {
    public void testTraceRequest() throws IOException, URISyntaxException, ParseException {
        HttpHost host = new HttpHost(randomBoolean() ? "http" : "https", "localhost", 9200);
        String expectedEndpoint = "/index/type/_api";
        URI uri;
        if (randomBoolean()) {
            uri = new URI(expectedEndpoint);
        } else {
            uri = new URI("index/type/_api");
        }
        HttpUriRequest request = randomHttpRequest(uri);
        String expected = "curl -iX " + request.getMethod() + " '" + host + expectedEndpoint + "'";
        boolean hasBody = !(request instanceof HttpTrace) && randomBoolean();
        String requestBody = "{ \"field\": \"value\" }";
        if (hasBody) {
            expected += " -d '" + requestBody + "'";
            HttpEntity entity;
            switch (randomIntBetween(0, 4)) {
                case 0:
                    entity = new StringEntity(requestBody, ContentType.APPLICATION_JSON);
                    break;
                case 1:
                    entity = new InputStreamEntity(
                        new ByteArrayInputStream(requestBody.getBytes(StandardCharsets.UTF_8)),
                        ContentType.APPLICATION_JSON
                    );
                    break;
                case 2:
                    entity = new StringEntity(requestBody, ContentType.APPLICATION_JSON);
                    break;
                case 3:
                    entity = new ByteArrayEntity(requestBody.getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
                    break;
                case 4:
                    // Evil entity without a charset
                    entity = new StringEntity(requestBody, ContentType.create("application/json", (Charset) null));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            request.setEntity(entity);
        }
        String traceRequest = RequestLogger.buildTraceRequest(request, host);
        assertThat(traceRequest, equalTo(expected));
        if (hasBody) {
            // check that the body is still readable as most entities are not repeatable
            String body = EntityUtils.toString(request.getEntity(), StandardCharsets.UTF_8);
            assertThat(body, equalTo(requestBody));
        }
    }

    public void testTraceResponse() throws IOException, ParseException {
        ProtocolVersion protocolVersion = new ProtocolVersion("HTTP", 1, 1);
        int statusCode = randomIntBetween(200, 599);
        String reasonPhrase = "REASON";
        StatusLine statusLine = new StatusLine(protocolVersion, statusCode, reasonPhrase);
        String expected = "# " + statusLine.toString();
        ClassicHttpResponse httpResponse = new BasicClassicHttpResponse(statusCode, reasonPhrase);
        int numHeaders = randomIntBetween(0, 3);
        for (int i = 0; i < numHeaders; i++) {
            httpResponse.setHeader("header" + i, "value");
            expected += "\n# header" + i + ": value";
        }
        expected += "\n#";
        boolean hasBody = getRandom().nextBoolean();
        String responseBody = "{\n  \"field\": \"value\"\n}";
        if (hasBody) {
            expected += "\n# {";
            expected += "\n#   \"field\": \"value\"";
            expected += "\n# }";
            HttpEntity entity;
            switch (randomIntBetween(0, 2)) {
                case 0:
                    entity = new StringEntity(responseBody, ContentType.APPLICATION_JSON);
                    break;
                case 1:
                    // test a non repeatable entity
                    entity = new InputStreamEntity(
                        new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8)),
                        ContentType.APPLICATION_JSON
                    );
                    break;
                case 2:
                    // Evil entity without a charset
                    entity = new StringEntity(responseBody, ContentType.create("application/json", (Charset) null));
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            httpResponse.setEntity(entity);
        }
        String traceResponse = RequestLogger.buildTraceResponse(httpResponse);
        assertThat(traceResponse, equalTo(expected));
        if (hasBody) {
            // check that the body is still readable as most entities are not repeatable
            String body = EntityUtils.toString(httpResponse.getEntity(), StandardCharsets.UTF_8);
            assertThat(body, equalTo(responseBody));
        }
    }

    public void testResponseWarnings() throws Exception {
        HttpHost host = new HttpHost("localhost", 9200);
        HttpUriRequest request = randomHttpRequest(new URI("/index/type/_api"));
        int numWarnings = randomIntBetween(1, 5);
        StringBuilder expected = new StringBuilder("request [").append(request.getMethod())
            .append(" ")
            .append(host)
            .append("/index/type/_api] returned ")
            .append(numWarnings)
            .append(" warnings: ");
        Header[] warnings = new Header[numWarnings];
        for (int i = 0; i < numWarnings; i++) {
            String warning = "this is warning number " + i;
            warnings[i] = new BasicHeader("Warning", warning);
            if (i > 0) {
                expected.append(",");
            }
            expected.append("[").append(warning).append("]");
        }
        assertEquals(expected.toString(), RequestLogger.buildWarningMessage(request, host, warnings));
    }

    private static HttpUriRequest randomHttpRequest(URI uri) {
        int requestType = randomIntBetween(0, 7);
        switch (requestType) {
            case 0:
                return new HttpGet(uri);
            case 1:
                return new HttpPost(uri);
            case 2:
                return new HttpPut(uri);
            case 3:
                return new HttpDelete(uri);
            case 4:
                return new HttpHead(uri);
            case 5:
                return new HttpTrace(uri);
            case 6:
                return new HttpOptions(uri);
            case 7:
                return new HttpPatch(uri);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
