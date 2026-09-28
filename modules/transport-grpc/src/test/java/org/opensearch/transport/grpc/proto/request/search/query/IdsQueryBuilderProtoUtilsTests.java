/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.protobufs.IdsQuery;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

import static org.opensearch.transport.grpc.proto.request.search.query.IdsQueryBuilderProtoUtils.fromProto;

public class IdsQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        QueryBuilderProtoTestUtils.setupRegistry();
    }

    public void testFromProtoWithAllFields() {
        // Create a protobuf IdsQuery with all fields
        IdsQuery idsQuery = IdsQuery.newBuilder()
            .setXName("test_ids_query")
            .setBoost(1.5f)
            .addValues("doc1")
            .addValues("doc2")
            .addValues("doc3")
            .build();

        // Call the method under test
        IdsQueryBuilder result = fromProto(idsQuery);

        // Verify the result
        assertEquals("test_ids_query", result.queryName());
        assertEquals(1.5f, result.boost(), 0.001f);

        Set<String> ids = result.ids();
        assertEquals(3, ids.size());
        assertTrue(ids.contains("doc1"));
        assertTrue(ids.contains("doc2"));
        assertTrue(ids.contains("doc3"));
    }

    public void testFromProtoWithMinimalFields() {
        // Create a protobuf IdsQuery with only required fields
        IdsQuery idsQuery = IdsQuery.newBuilder().build();

        // Call the method under test
        IdsQueryBuilder result = fromProto(idsQuery);

        // Verify the result
        assertNull(result.queryName());
        assertEquals(1.0f, result.boost(), 0.001f);
        assertEquals(0, result.ids().size());
    }

    public void testFromProtoWithBoostOnly() {
        // Create a protobuf IdsQuery with only boost set
        IdsQuery idsQuery = IdsQuery.newBuilder().setBoost(2.5f).build();

        // Call the method under test
        IdsQueryBuilder result = fromProto(idsQuery);

        // Verify the result
        assertNull(result.queryName());
        assertEquals(2.5f, result.boost(), 0.001f);
        assertEquals(0, result.ids().size());
    }

    public void testFromProtoWithNameOnly() {
        // Create a protobuf IdsQuery with only name set
        IdsQuery idsQuery = IdsQuery.newBuilder().setXName("my_ids_query").build();

        // Call the method under test
        IdsQueryBuilder result = fromProto(idsQuery);

        // Verify the result
        assertEquals("my_ids_query", result.queryName());
        assertEquals(1.0f, result.boost(), 0.001f);
        assertEquals(0, result.ids().size());
    }

    public void testFromProtoWithValuesOnly() {
        // Create a protobuf IdsQuery with only values set
        IdsQuery idsQuery = IdsQuery.newBuilder().addValues("id1").addValues("id2").build();

        // Call the method under test
        IdsQueryBuilder result = fromProto(idsQuery);

        // Verify the result
        assertNull(result.queryName());
        assertEquals(1.0f, result.boost(), 0.001f);

        Set<String> ids = result.ids();
        assertEquals(2, ids.size());
        assertTrue(ids.contains("id1"));
        assertTrue(ids.contains("id2"));
    }

    public void testFromProtoWithSingleValue() {
        // Create a protobuf IdsQuery with a single value
        IdsQuery idsQuery = IdsQuery.newBuilder().addValues("single_doc_id").build();

        // Call the method under test
        IdsQueryBuilder result = fromProto(idsQuery);

        // Verify the result
        Set<String> ids = result.ids();
        assertEquals(1, ids.size());
        assertTrue(ids.contains("single_doc_id"));
    }

    public void testFromProtoWithDuplicateValues() {
        // Create a protobuf IdsQuery with duplicate values
        IdsQuery idsQuery = IdsQuery.newBuilder()
            .addValues("doc1")
            .addValues("doc2")
            .addValues("doc1") // duplicate
            .build();

        // Call the method under test
        IdsQueryBuilder result = fromProto(idsQuery);

        // Verify the result - IdsQueryBuilder uses a Set internally, so duplicates should be removed
        Set<String> ids = result.ids();
        assertEquals(2, ids.size()); // Should only have 2 unique values
        assertTrue(ids.contains("doc1"));
        assertTrue(ids.contains("doc2"));
    }

    public void testFromProtoWithEmptyStringValue() {
        // Create a protobuf IdsQuery with empty string value
        IdsQuery idsQuery = IdsQuery.newBuilder().addValues("").addValues("doc1").build();

        // Call the method under test
        IdsQueryBuilder result = fromProto(idsQuery);

        // Verify the result
        Set<String> ids = result.ids();
        assertEquals(2, ids.size());
        assertTrue(ids.contains(""));
        assertTrue(ids.contains("doc1"));
    }

    public void testFromProtoWithZeroBoost() {
        // Create a protobuf IdsQuery with zero boost
        IdsQuery idsQuery = IdsQuery.newBuilder().setBoost(0.0f).addValues("doc1").build();

        // Call the method under test
        IdsQueryBuilder result = fromProto(idsQuery);

        // Verify the result
        assertEquals(0.0f, result.boost(), 0.001f);
        assertEquals(1, result.ids().size());
    }

    public void testFromProtoWithNegativeBoost() {
        // Create a protobuf IdsQuery with negative boost
        IdsQuery idsQuery = IdsQuery.newBuilder().setBoost(-1.0f).addValues("doc1").build();

        // Call the method under test - should throw IllegalArgumentException for negative boost
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { fromProto(idsQuery); });

        // Verify the exception message
        assertTrue("Exception message should mention negative boost", exception.getMessage().contains("negative [boost] are not allowed"));
    }
}
