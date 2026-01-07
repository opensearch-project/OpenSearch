/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.test;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for gRPC test framework utilities.
 */
public class GrpcTestFrameworkTests {

    @Test
    public void testCreateTestDocument() {
        String doc = GrpcOpenSearchIntegTestCase.createTestDocument("name", "test-value");
        assertEquals("Should create proper JSON", "{\"name\": \"test-value\"}", doc);
    }

    @Test
    public void testCreateTestDocuments() {
        List<String> docs = GrpcOpenSearchIntegTestCase.createTestDocuments("field", "doc", 3);
        assertEquals("Should create 3 documents", 3, docs.size());
        assertEquals("First doc should be correct", "{\"field\": \"doc 0\"}", docs.get(0));
        assertEquals("Second doc should be correct", "{\"field\": \"doc 1\"}", docs.get(1));
        assertEquals("Last doc should be correct", "{\"field\": \"doc 2\"}", docs.get(2));
    }
}
