/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.IdsQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.test.OpenSearchTestCase;

public class IdsQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private IdsQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new IdsQueryBuilderProtoConverter();
        QueryBuilderProtoTestUtils.setupRegistry();
    }

    public void testGetHandledQueryCase() {
        assertEquals(QueryContainer.QueryContainerCase.IDS, converter.getHandledQueryCase());
    }

    public void testFromProtoWithValidIdsQuery() {
        // Create a valid IdsQuery
        IdsQuery idsQuery = IdsQuery.newBuilder().setXName("test_query").setBoost(1.5f).addValues("doc1").addValues("doc2").build();

        // Create QueryContainer with IdsQuery
        QueryContainer queryContainer = QueryContainer.newBuilder().setIds(idsQuery).build();

        // Call the method under test
        QueryBuilder result = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull(result);
        assertTrue(result instanceof IdsQueryBuilder);

        IdsQueryBuilder idsQueryBuilder = (IdsQueryBuilder) result;
        assertEquals("test_query", idsQueryBuilder.queryName());
        assertEquals(1.5f, idsQueryBuilder.boost(), 0.001f);
        assertEquals(2, idsQueryBuilder.ids().size());
        assertTrue(idsQueryBuilder.ids().contains("doc1"));
        assertTrue(idsQueryBuilder.ids().contains("doc2"));
    }

    public void testFromProtoWithMinimalIdsQuery() {
        // Create a minimal IdsQuery
        IdsQuery idsQuery = IdsQuery.newBuilder().build();

        // Create QueryContainer with IdsQuery
        QueryContainer queryContainer = QueryContainer.newBuilder().setIds(idsQuery).build();

        // Call the method under test
        QueryBuilder result = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull(result);
        assertTrue(result instanceof IdsQueryBuilder);

        IdsQueryBuilder idsQueryBuilder = (IdsQueryBuilder) result;
        assertNull(idsQueryBuilder.queryName());
        assertEquals(1.0f, idsQueryBuilder.boost(), 0.001f);
        assertEquals(0, idsQueryBuilder.ids().size());
    }

    public void testFromProtoWithNullQueryContainer() {
        // Test with null QueryContainer
        expectThrows(IllegalArgumentException.class, () -> { converter.fromProto(null); });
    }

    public void testFromProtoWithoutIdsQuery() {
        // Create QueryContainer without IdsQuery (empty)
        QueryContainer queryContainer = QueryContainer.newBuilder().build();

        // Test should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { converter.fromProto(queryContainer); });

        assertTrue(exception.getMessage().contains("QueryContainer does not contain an Ids query"));
    }

    public void testFromProtoWithDifferentQueryType() {
        // Create QueryContainer with a different query type (not IdsQuery)
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setBool(org.opensearch.protobufs.BoolQuery.newBuilder().build())
            .build();

        // Test should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { converter.fromProto(queryContainer); });

        assertTrue(exception.getMessage().contains("QueryContainer does not contain an Ids query"));
    }

    public void testFromProtoWithComplexIdsQuery() {
        // Create a complex IdsQuery with many values
        IdsQuery.Builder idsProtoBuilder = IdsQuery.newBuilder().setXName("complex_ids_query").setBoost(2.0f);

        // Add multiple values
        for (int i = 0; i < 10; i++) {
            idsProtoBuilder.addValues("doc_" + i);
        }

        IdsQuery idsQuery = idsProtoBuilder.build();

        // Create QueryContainer with IdsQuery
        QueryContainer queryContainer = QueryContainer.newBuilder().setIds(idsQuery).build();

        // Call the method under test
        QueryBuilder result = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull(result);
        assertTrue(result instanceof IdsQueryBuilder);

        IdsQueryBuilder idsQueryBuilder = (IdsQueryBuilder) result;
        assertEquals("complex_ids_query", idsQueryBuilder.queryName());
        assertEquals(2.0f, idsQueryBuilder.boost(), 0.001f);
        assertEquals(10, idsQueryBuilder.ids().size());

        // Verify all IDs are present
        for (int i = 0; i < 10; i++) {
            assertTrue(idsQueryBuilder.ids().contains("doc_" + i));
        }
    }
}
