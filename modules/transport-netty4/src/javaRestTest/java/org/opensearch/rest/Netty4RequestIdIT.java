/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class Netty4RequestIdIT extends OpenSearchRestTestCase {

    private Response requestWithId(String requestId) throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("X-Request-Id", requestId);
        request.setOptions(options);
        return client().performRequest(request);
    }

    public void testRequestIdExactlyAtMax() throws IOException {
        assertThat(requestWithId("a".repeat(128)).getStatusLine().getStatusCode(), equalTo(200));
    }

    public void testRequestIdTooLong() {
        ResponseException e = expectThrows(ResponseException.class, () -> requestWithId("a".repeat(129)));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("exceeds maximum allowed length"));
    }

    public void testRequestIdAfterSettingUpdate() throws IOException {
        int newMax = 20;

        // Expect request is valid under default
        assertThat(requestWithId("a".repeat(128)).getStatusLine().getStatusCode(), equalTo(200));

        // Update setting
        Request updateSettings = new Request("PUT", "/_cluster/settings");
        updateSettings.setJsonEntity("{\"transient\": {\"http.request_id.max_length\": " + newMax + "}}");
        client().performRequest(updateSettings);

        // Was valid under default, now too long
        ResponseException e = expectThrows(ResponseException.class, () -> requestWithId("a".repeat(129)));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("exceeds maximum allowed length [" + newMax + "]"));

        // ID at new size passes
        assertThat(requestWithId("a".repeat(newMax)).getStatusLine().getStatusCode(), equalTo(200));
    }
}
