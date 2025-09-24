/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.BoolQuery;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.MinimumShouldMatch;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.transport.grpc.proto.request.search.query.BoolQueryBuilderProtoUtils.fromProto;

public class BoolQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        QueryBuilderProtoConverterRegistryImpl registry = new QueryBuilderProtoConverterRegistryImpl();
        BoolQueryBuilderProtoUtils.setRegistry(registry);
    }

    public void testFromProtoWithAllFields() {
        // Create a protobuf BoolQuery with all fields
        BoolQuery boolQuery = BoolQuery.newBuilder()
            .setXName("test_query")
            .setBoost(2.0f)
            .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().setInt32Value(2).build())
            .addMust(createTermQueryContainer("field1", "value1"))
            .addMustNot(createTermQueryContainer("field2", "value2"))
            .addShould(createTermQueryContainer("field3", "value3"))
            .addShould(createTermQueryContainer("field4", "value4"))
            .addFilter(createMatchAllQueryContainer())
            .build();

        // Call the method under test
        BoolQueryBuilder result = fromProto(boolQuery);

        // Verify the result
        assertEquals("test_query", result.queryName());
        assertEquals(2.0f, result.boost(), 0.001f);
        assertEquals("2", result.minimumShouldMatch());
        assertEquals(1, result.must().size());
        assertEquals(1, result.mustNot().size());
        assertEquals(2, result.should().size());
        assertEquals(1, result.filter().size());

        // Verify the must clause
        assertTrue(result.must().get(0) instanceof TermQueryBuilder);
        TermQueryBuilder mustClause = (TermQueryBuilder) result.must().get(0);
        assertEquals("field1", mustClause.fieldName());
        assertEquals("value1", mustClause.value());

        // Verify the must_not clause
        assertTrue(result.mustNot().get(0) instanceof TermQueryBuilder);
        TermQueryBuilder mustNotClause = (TermQueryBuilder) result.mustNot().get(0);
        assertEquals("field2", mustNotClause.fieldName());
        assertEquals("value2", mustNotClause.value());

        // Verify the should clauses
        assertTrue(result.should().get(0) instanceof TermQueryBuilder);
        TermQueryBuilder shouldClause1 = (TermQueryBuilder) result.should().get(0);
        assertEquals("field3", shouldClause1.fieldName());
        assertEquals("value3", shouldClause1.value());

        assertTrue(result.should().get(1) instanceof TermQueryBuilder);
        TermQueryBuilder shouldClause2 = (TermQueryBuilder) result.should().get(1);
        assertEquals("field4", shouldClause2.fieldName());
        assertEquals("value4", shouldClause2.value());

        // Verify the filter clause
        assertTrue(result.filter().get(0) instanceof MatchAllQueryBuilder);
    }

    public void testFromProtoWithMinimalFields() {
        // Create a protobuf BoolQuery with only required fields
        BoolQuery boolQuery = BoolQuery.newBuilder().build();

        // Call the method under test
        BoolQueryBuilder result = fromProto(boolQuery);

        // Verify the result
        assertNull(result.queryName());
        assertEquals(1.0f, result.boost(), 0.001f);
        assertNull(result.minimumShouldMatch());
        assertEquals(0, result.must().size());
        assertEquals(0, result.mustNot().size());
        assertEquals(0, result.should().size());
        assertEquals(0, result.filter().size());
    }

    public void testFromProtoWithStringMinimumShouldMatch() {
        // Create a protobuf BoolQuery with string minimum_should_match
        BoolQuery boolQuery = BoolQuery.newBuilder()
            .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().setStringValue("75%").build())
            .build();

        // Call the method under test
        BoolQueryBuilder result = fromProto(boolQuery);

        // Verify the result
        assertEquals("75%", result.minimumShouldMatch());
    }

    public void testFromProtoWithMustClauses() {
        // Create a protobuf BoolQuery with multiple must clauses
        BoolQuery boolQuery = BoolQuery.newBuilder()
            .addMust(createTermQueryContainer("field1", "value1"))
            .addMust(createTermQueryContainer("field2", "value2"))
            .build();

        // Call the method under test
        BoolQueryBuilder result = fromProto(boolQuery);

        // Verify the result
        assertEquals(2, result.must().size());

        // Verify the must clauses
        assertTrue(result.must().get(0) instanceof TermQueryBuilder);
        TermQueryBuilder mustClause1 = (TermQueryBuilder) result.must().get(0);
        assertEquals("field1", mustClause1.fieldName());
        assertEquals("value1", mustClause1.value());

        assertTrue(result.must().get(1) instanceof TermQueryBuilder);
        TermQueryBuilder mustClause2 = (TermQueryBuilder) result.must().get(1);
        assertEquals("field2", mustClause2.fieldName());
        assertEquals("value2", mustClause2.value());
    }

    public void testFromProtoWithMustNotClauses() {
        // Create a protobuf BoolQuery with multiple must_not clauses
        BoolQuery boolQuery = BoolQuery.newBuilder()
            .addMustNot(createTermQueryContainer("field1", "value1"))
            .addMustNot(createTermQueryContainer("field2", "value2"))
            .build();

        // Call the method under test
        BoolQueryBuilder result = fromProto(boolQuery);

        // Verify the result
        assertEquals(2, result.mustNot().size());

        // Verify the must_not clauses
        assertTrue(result.mustNot().get(0) instanceof TermQueryBuilder);
        TermQueryBuilder mustNotClause1 = (TermQueryBuilder) result.mustNot().get(0);
        assertEquals("field1", mustNotClause1.fieldName());
        assertEquals("value1", mustNotClause1.value());

        assertTrue(result.mustNot().get(1) instanceof TermQueryBuilder);
        TermQueryBuilder mustNotClause2 = (TermQueryBuilder) result.mustNot().get(1);
        assertEquals("field2", mustNotClause2.fieldName());
        assertEquals("value2", mustNotClause2.value());
    }

    public void testFromProtoWithShouldClauses() {
        // Create a protobuf BoolQuery with multiple should clauses
        BoolQuery boolQuery = BoolQuery.newBuilder()
            .addShould(createTermQueryContainer("field1", "value1"))
            .addShould(createTermQueryContainer("field2", "value2"))
            .build();

        // Call the method under test
        BoolQueryBuilder result = fromProto(boolQuery);

        // Verify the result
        assertEquals(2, result.should().size());

        // Verify the should clauses
        assertTrue(result.should().get(0) instanceof TermQueryBuilder);
        TermQueryBuilder shouldClause1 = (TermQueryBuilder) result.should().get(0);
        assertEquals("field1", shouldClause1.fieldName());
        assertEquals("value1", shouldClause1.value());

        assertTrue(result.should().get(1) instanceof TermQueryBuilder);
        TermQueryBuilder shouldClause2 = (TermQueryBuilder) result.should().get(1);
        assertEquals("field2", shouldClause2.fieldName());
        assertEquals("value2", shouldClause2.value());
    }

    public void testFromProtoWithFilterClauses() {
        // Create a protobuf BoolQuery with multiple filter clauses
        BoolQuery boolQuery = BoolQuery.newBuilder()
            .addFilter(createTermQueryContainer("field1", "value1"))
            .addFilter(createTermQueryContainer("field2", "value2"))
            .build();

        // Call the method under test
        BoolQueryBuilder result = fromProto(boolQuery);

        // Verify the result
        assertEquals(2, result.filter().size());

        // Verify the filter clauses
        assertTrue(result.filter().get(0) instanceof TermQueryBuilder);
        TermQueryBuilder filterClause1 = (TermQueryBuilder) result.filter().get(0);
        assertEquals("field1", filterClause1.fieldName());
        assertEquals("value1", filterClause1.value());

        assertTrue(result.filter().get(1) instanceof TermQueryBuilder);
        TermQueryBuilder filterClause2 = (TermQueryBuilder) result.filter().get(1);
        assertEquals("field2", filterClause2.fieldName());
        assertEquals("value2", filterClause2.value());
    }

    private QueryContainer createTermQueryContainer(String field, String value) {
        return QueryContainer.newBuilder()
            .setTerm(TermQuery.newBuilder().setField(field).setValue(FieldValue.newBuilder().setString(value).build()).build())
            .build();
    }

    private QueryContainer createMatchAllQueryContainer() {
        return QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();
    }
}
