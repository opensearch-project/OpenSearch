/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.indices.TermsLookup;
import org.opensearch.test.OpenSearchTestCase;

public class TermsLookupProtoUtilsTests extends OpenSearchTestCase {

    public void testParseTermsLookupWithBasicFields() {
        // Create a TermsLookup instance with basic fields (TermsLookupField was renamed to TermsLookup in protobufs 1.0.0)
        org.opensearch.protobufs.TermsLookup termsLookupProto = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupProto);

        // Verify the result
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should match", "test_index", termsLookup.index());
        assertEquals("ID should match", "test_id", termsLookup.id());
        assertEquals("Path should match", "test_path", termsLookup.path());
        assertNull("Routing should be null", termsLookup.routing());
        assertFalse("Store should be false by default", termsLookup.store());
    }

    public void testParseTermsLookupWithStore() {
        // Create a TermsLookup instance with store field
        org.opensearch.protobufs.TermsLookup termsLookupProto = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .setStore(true)
            .build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupProto);

        // Verify the result
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should match", "test_index", termsLookup.index());
        assertEquals("ID should match", "test_id", termsLookup.id());
        assertEquals("Path should match", "test_path", termsLookup.path());
        assertNull("Routing should be null", termsLookup.routing());
        assertTrue("Store should be true", termsLookup.store());
    }

    public void testParseTermsLookupWithNullInput() {
        // Call the method under test with null input, should throw NullPointerException
        NullPointerException exception = expectThrows(NullPointerException.class, () -> TermsLookupProtoUtils.parseTermsLookup(null));
    }

    // This test verifies the bug fix for using index instead of id
    public void testParseTermsLookupWithDifferentIndexAndId() {
        // Create a TermsLookup instance with different index and id values
        org.opensearch.protobufs.TermsLookup termsLookupProto = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupProto);

        // Verify the result
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should match", "test_index", termsLookup.index());
        assertEquals("ID should match", "test_id", termsLookup.id());
        assertEquals("Path should match", "test_path", termsLookup.path());
    }

    public void testParseTermsLookupWithEmptyFields() {
        // Create a TermsLookup instance with empty fields
        org.opensearch.protobufs.TermsLookup termsLookupProto = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("")
            .setId("")
            .setPath("")
            .build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupProto);

        // Verify the result
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should be empty", "", termsLookup.index());
        assertEquals("ID should be empty", "", termsLookup.id());
        assertEquals("Path should be empty", "", termsLookup.path());
    }

    public void testParseTermsLookupWithRouting() {
        // Create a TermsLookup instance with routing field
        org.opensearch.protobufs.TermsLookup termsLookupProto = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .setRouting("test_routing")
            .build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupProto);

        // Verify the result
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should match", "test_index", termsLookup.index());
        assertEquals("ID should match", "test_id", termsLookup.id());
        assertEquals("Path should match", "test_path", termsLookup.path());
        assertEquals("Routing should match", "test_routing", termsLookup.routing());
        assertFalse("Store should be false by default", termsLookup.store());
    }

    public void testParseTermsLookupWithRoutingAndStore() {
        // Create a TermsLookup instance with both routing and store fields
        org.opensearch.protobufs.TermsLookup termsLookupProto = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .setRouting("test_routing")
            .setStore(true)
            .build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupProto);

        // Verify the result
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should match", "test_index", termsLookup.index());
        assertEquals("ID should match", "test_id", termsLookup.id());
        assertEquals("Path should match", "test_path", termsLookup.path());
        assertEquals("Routing should match", "test_routing", termsLookup.routing());
        assertTrue("Store should be true", termsLookup.store());
    }
}
