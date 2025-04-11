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

public class WorkloadGroupThreadContextStatePropagatorTests extends OpenSearchTestCase {

    public void testTransients() {
        WorkloadGroupThreadContextStatePropagator sut = new WorkloadGroupThreadContextStatePropagator();
        Map<String, Object> source = Map.of("workloadGroupId", "adgarja0r235te");
        Map<String, Object> transients = sut.transients(source);
        assertEquals("adgarja0r235te", transients.get("workloadGroupId"));
    }

    public void testHeaders() {
        WorkloadGroupThreadContextStatePropagator sut = new WorkloadGroupThreadContextStatePropagator();
        Map<String, Object> source = Map.of("workloadGroupId", "adgarja0r235te");
        Map<String, String> headers = sut.headers(source);
        assertEquals("adgarja0r235te", headers.get("workloadGroupId"));
    }
}
