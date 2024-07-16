/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class QueryGroupThreadContextStatePropagatorTests extends OpenSearchTestCase {

    public void testTransients() {
        QueryGroupThreadContextStatePropagator sut = new QueryGroupThreadContextStatePropagator();
        Map<String, Object> source = Map.of("queryGroupId", "adgarja0r235te");
        Map<String, Object> transients = sut.transients(source);
        assertEquals("adgarja0r235te", transients.get("queryGroupId"));
    }

    public void testHeaders() {
        QueryGroupThreadContextStatePropagator sut = new QueryGroupThreadContextStatePropagator();
        Map<String, Object> source = Map.of("queryGroupId", "adgarja0r235te");
        Map<String, String> headers = sut.headers(source);
        assertEquals("adgarja0r235te", headers.get("queryGroupId"));
    }
}
