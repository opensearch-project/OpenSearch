/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.protobufs.ConstantScoreQuery;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;

public class ConstantScoreQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistryImpl registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProtoWithBasicConstantScoreQuery() {
        // Create a simple term query to wrap
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("status")
            .setValue(FieldValue.newBuilder().setString("active").build())
            .build();
        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        ConstantScoreQuery constantScoreQuery = ConstantScoreQuery.newBuilder().setFilter(innerQueryContainer).build();

        ConstantScoreQueryBuilder constantScoreQueryBuilder = ConstantScoreQueryBuilderProtoUtils.fromProto(constantScoreQuery, registry);

        assertNotNull("ConstantScoreQueryBuilder should not be null", constantScoreQueryBuilder);
        QueryBuilder innerQuery = constantScoreQueryBuilder.innerQuery();
        assertNotNull("Inner query should not be null", innerQuery);
        assertTrue("Inner query should be a TermQueryBuilder", innerQuery instanceof TermQueryBuilder);

        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) innerQuery;
        assertEquals("Inner query field should match", "status", termQueryBuilder.fieldName());
        assertEquals("Inner query value should match", "active", termQueryBuilder.value());
        assertEquals("Default boost should be 1.0", 1.0f, constantScoreQueryBuilder.boost(), 0.0f);
    }

    public void testFromProtoWithAllParameters() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("status")
            .setValue(FieldValue.newBuilder().setString("active").build())
            .build();
        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        ConstantScoreQuery constantScoreQuery = ConstantScoreQuery.newBuilder()
            .setFilter(innerQueryContainer)
            .setBoost(2.5f)
            .setXName("test_constant_score")
            .build();

        ConstantScoreQueryBuilder constantScoreQueryBuilder = ConstantScoreQueryBuilderProtoUtils.fromProto(constantScoreQuery, registry);

        assertNotNull("ConstantScoreQueryBuilder should not be null", constantScoreQueryBuilder);
        QueryBuilder innerQuery = constantScoreQueryBuilder.innerQuery();
        assertNotNull("Inner query should not be null", innerQuery);
        assertTrue("Inner query should be a TermQueryBuilder", innerQuery instanceof TermQueryBuilder);
        assertEquals("Boost should match", 2.5f, constantScoreQueryBuilder.boost(), 0.001f);
        assertEquals("Query name should match", "test_constant_score", constantScoreQueryBuilder.queryName());
    }

    public void testFromProtoWithMinimalFields() {
        TermQuery termQuery = TermQuery.newBuilder().setField("active").setValue(FieldValue.newBuilder().setBool(true).build()).build();
        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        ConstantScoreQuery constantScoreQuery = ConstantScoreQuery.newBuilder().setFilter(innerQueryContainer).build();

        ConstantScoreQueryBuilder constantScoreQueryBuilder = ConstantScoreQueryBuilderProtoUtils.fromProto(constantScoreQuery, registry);

        assertNotNull("ConstantScoreQueryBuilder should not be null", constantScoreQueryBuilder);
        assertNotNull("Inner query should not be null", constantScoreQueryBuilder.innerQuery());
        assertNull("Query name should be null", constantScoreQueryBuilder.queryName());
    }

    public void testFromProtoWithNullRegistry() {
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("status")
            .setValue(FieldValue.newBuilder().setString("active").build())
            .build();
        QueryContainer innerQueryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();
        ConstantScoreQuery constantScoreQuery = ConstantScoreQuery.newBuilder().setFilter(innerQueryContainer).build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ConstantScoreQueryBuilderProtoUtils.fromProto(constantScoreQuery, null)
        );

        assertEquals("QueryBuilderProtoConverterRegistry cannot be null", exception.getMessage());
    }

    public void testFromProtoWithMissingFilter() {
        ConstantScoreQuery constantScoreQuery = ConstantScoreQuery.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ConstantScoreQueryBuilderProtoUtils.fromProto(constantScoreQuery, registry)
        );

        assertEquals("ConstantScore query must have a filter query", exception.getMessage());
    }
}
