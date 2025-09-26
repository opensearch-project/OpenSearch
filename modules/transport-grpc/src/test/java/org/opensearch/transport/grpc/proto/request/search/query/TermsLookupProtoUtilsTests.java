/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.indices.TermsLookup;
import org.opensearch.protobufs.TermsLookupField;
import org.opensearch.test.OpenSearchTestCase;

public class TermsLookupProtoUtilsTests extends OpenSearchTestCase {

    public void testParseTermsLookupWithBasicFields() {
        // Create a TermsLookupField instance with basic fields
        TermsLookupField termsLookupField = TermsLookupField.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupField);

        // Verify the result
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should match", "test_index", termsLookup.index());
        assertEquals("ID should match", "test_id", termsLookup.id());
        assertEquals("Path should match", "test_path", termsLookup.path());
        assertNull("Routing should be null", termsLookup.routing());
        assertFalse("Store should be false by default", termsLookup.store());
    }

    public void testParseTermsLookupWithStore() {
        // Create a TermsLookupField instance with store field
        TermsLookupField termsLookupField = TermsLookupField.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .setStore(true)
            .build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupField);

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
        // Create a TermsLookupField instance with different index and id values
        TermsLookupField termsLookupField = TermsLookupField.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupField);

        // Verify the result
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should match", "test_index", termsLookup.index());
        assertEquals("ID should match", "test_id", termsLookup.id());
        assertEquals("Path should match", "test_path", termsLookup.path());
    }

    public void testParseTermsLookupWithEmptyFields() {
        // Create a TermsLookupField instance with empty fields
        TermsLookupField termsLookupField = TermsLookupField.newBuilder().setIndex("").setId("").setPath("").build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupField);

        // Verify the result
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should be empty", "", termsLookup.index());
        assertEquals("ID should be empty", "", termsLookup.id());
        assertEquals("Path should be empty", "", termsLookup.path());
    }

    public void testParseTermsLookupWithRouting() {
        // Create a TermsLookupField instance with routing field
        TermsLookupField termsLookupField = TermsLookupField.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .setRouting("test_routing")
            .build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupField);

        // Verify the result
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should match", "test_index", termsLookup.index());
        assertEquals("ID should match", "test_id", termsLookup.id());
        assertEquals("Path should match", "test_path", termsLookup.path());
        assertEquals("Routing should match", "test_routing", termsLookup.routing());
        assertFalse("Store should be false by default", termsLookup.store());
    }

    public void testParseTermsLookupWithRoutingAndStore() {
        // Create a TermsLookupField instance with both routing and store fields
        TermsLookupField termsLookupField = TermsLookupField.newBuilder()
            .setIndex("test_index")
            .setId("test_id")
            .setPath("test_path")
            .setRouting("test_routing")
            .setStore(true)
            .build();

        // Call the method under test
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupField);

        // Verify the result
        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should match", "test_index", termsLookup.index());
        assertEquals("ID should match", "test_id", termsLookup.id());
        assertEquals("Path should match", "test_path", termsLookup.path());
        assertEquals("Routing should match", "test_routing", termsLookup.routing());
        assertTrue("Store should be true", termsLookup.store());
    }
}
