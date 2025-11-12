/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermsSetQueryBuilder;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermsSetQuery;
import org.opensearch.test.OpenSearchTestCase;

public class TermsSetQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private TermsSetQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new TermsSetQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        assertEquals(QueryContainer.QueryContainerCase.TERMS_SET, converter.getHandledQueryCase());
    }

    public void testFromProtoValid() {
        TermsSetQuery termsSetQuery = TermsSetQuery.newBuilder()
            .setField("status")
            .addTerms("published")
            .setMinimumShouldMatchField("count")
            .setBoost(1.5f)
            .setXName("status_query")
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTermsSet(termsSetQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertNotNull(result);
        assertTrue(result instanceof TermsSetQueryBuilder);

        TermsSetQueryBuilder termsSetResult = (TermsSetQueryBuilder) result;
        assertEquals(1, termsSetResult.getValues().size());
        Object value = termsSetResult.getValues().get(0);
        String stringValue = value instanceof org.apache.lucene.util.BytesRef
            ? ((org.apache.lucene.util.BytesRef) value).utf8ToString()
            : value.toString();
        assertEquals("published", stringValue);
        assertEquals(1.5f, termsSetResult.boost(), 0.0f);
        assertEquals("status_query", termsSetResult.queryName());
    }

    public void testFromProtoWithNullInput() {
        expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));
    }

    public void testFromProtoWithoutTermsSet() {
        QueryContainer queryContainer = QueryContainer.newBuilder().build();
        expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));
    }

    public void testFromProtoWithDifferentQueryType() {
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setMatchAll(org.opensearch.protobufs.MatchAllQuery.newBuilder().build())
            .build();

        expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));
    }
}
