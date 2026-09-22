/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;

/**
 * Integration tests verifying that unsupported functions return HTTP 400
 * with a user-friendly error message.
 */
public class UnsupportedFunctionIT extends OpenSearchRestTestCase {

    private static boolean provisioned = false;

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    private void ensureProvisioned() throws IOException {
        if (provisioned == false) {
            DatasetProvisioner.provision(client(), new Dataset("calcs", "calcs"));
            provisioned = true;
        }
    }

    public void testUnsupportedScalarFunctionReturns400() throws Exception {
        ensureProvisioned();

        ResponseException e = expectThrows(ResponseException.class, () -> {
            Request r = new Request("POST", "/_analytics/ppl");
            r.setJsonEntity("{\"query\": \"source=calcs | eval x = MATCH(str0, 'hello')\"}");
            client().performRequest(r);
        });

        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        String body = new String(e.getResponse().getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        assertTrue("error type should be unsupported_function_exception: " + body,
            body.contains("unsupported_function_exception"));
        assertTrue("error should mention function name MATCH: " + body,
            body.contains("Function [MATCH] is not currently supported"));
    }

    public void testUnsupportedScalarDoesNotLeakBackendNames() throws Exception {
        ensureProvisioned();

        ResponseException e = expectThrows(ResponseException.class, () -> {
            Request r = new Request("POST", "/_analytics/ppl");
            r.setJsonEntity("{\"query\": \"source=calcs | eval x = MATCH(str0, 'hello')\"}");
            client().performRequest(r);
        });

        String body = new String(e.getResponse().getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        assertFalse("error should not mention internal backend 'datafusion': " + body,
            body.contains("datafusion"));
        assertFalse("error should not mention internal backend 'lucene': " + body,
            body.contains("lucene"));
    }
}
