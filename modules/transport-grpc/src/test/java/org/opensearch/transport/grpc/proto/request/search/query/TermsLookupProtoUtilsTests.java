/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
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

    public void testParseTermsLookupWithId2() {
        // protobufs 1.7.0 replaced the deprecated `id` with the `id_2`/`query` oneof
        org.opensearch.protobufs.TermsLookup termsLookupProto = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("test_index")
            .setId2("lookup_id")
            .setPath("test_path")
            .build();

        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupProto);

        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should match", "test_index", termsLookup.index());
        assertEquals("ID should come from id_2", "lookup_id", termsLookup.id());
        assertEquals("Path should match", "test_path", termsLookup.path());
        assertNull("Query should not be set", termsLookup.query());
    }

    public void testParseTermsLookupWithQuery() {
        // A query-based terms lookup requires a registry to convert the inner query
        TermQuery termQuery = TermQuery.newBuilder().setField("color").setValue(FieldValue.newBuilder().setString("red").build()).build();
        QueryContainer innerQuery = QueryContainer.newBuilder().setTerm(termQuery).build();

        org.opensearch.protobufs.TermsLookup termsLookupProto = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("test_index")
            .setPath("test_path")
            .setQuery(innerQuery)
            .build();

        QueryBuilderProtoConverterRegistryImpl registry = new QueryBuilderProtoConverterRegistryImpl();
        TermsLookup termsLookup = TermsLookupProtoUtils.parseTermsLookup(termsLookupProto, registry);

        assertNotNull("TermsLookup should not be null", termsLookup);
        assertEquals("Index should match", "test_index", termsLookup.index());
        assertEquals("Path should match", "test_path", termsLookup.path());
        assertNull("ID should not be set for a query-based lookup", termsLookup.id());
        assertNotNull("Query should be set", termsLookup.query());
        assertTrue("Query should be a TermQueryBuilder", termsLookup.query() instanceof TermQueryBuilder);
        TermQueryBuilder queryBuilder = (TermQueryBuilder) termsLookup.query();
        assertEquals("Query field should match", "color", queryBuilder.fieldName());
        assertEquals("Query value should match", "red", queryBuilder.value());
    }

    public void testParseTermsLookupWithQueryButNoRegistry() {
        TermQuery termQuery = TermQuery.newBuilder().setField("color").setValue(FieldValue.newBuilder().setString("red").build()).build();
        QueryContainer innerQuery = QueryContainer.newBuilder().setTerm(termQuery).build();

        org.opensearch.protobufs.TermsLookup termsLookupProto = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex("test_index")
            .setPath("test_path")
            .setQuery(innerQuery)
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TermsLookupProtoUtils.parseTermsLookup(termsLookupProto, null)
        );
        assertTrue("Exception should mention registry", exception.getMessage().contains("registry"));
    }
}
