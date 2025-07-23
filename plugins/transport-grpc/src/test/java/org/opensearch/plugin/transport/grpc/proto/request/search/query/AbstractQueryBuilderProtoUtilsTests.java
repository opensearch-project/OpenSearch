/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.MatchNoneQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.StringArray;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.protobufs.TermsLookupFieldStringArrayMap;
import org.opensearch.protobufs.TermsQueryField;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class AbstractQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Set up the registry with all built-in converters
        QueryBuilderProtoTestUtils.setupRegistry();
    }

    public void testParseInnerQueryBuilderProtoWithMatchAll() {
        // Create a QueryContainer with MatchAllQuery
        MatchAllQuery matchAllQuery = MatchAllQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchAll(matchAllQuery).build();

        // Call parseInnerQueryBuilderProto
        QueryBuilder queryBuilder = AbstractQueryBuilderProtoUtils.parseInnerQueryBuilderProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchAllQueryBuilder", queryBuilder instanceof MatchAllQueryBuilder);
    }

    public void testParseInnerQueryBuilderProtoWithMatchNone() {
        // Create a QueryContainer with MatchNoneQuery
        MatchNoneQuery matchNoneQuery = MatchNoneQuery.newBuilder().build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setMatchNone(matchNoneQuery).build();

        // Call parseInnerQueryBuilderProto
        QueryBuilder queryBuilder = AbstractQueryBuilderProtoUtils.parseInnerQueryBuilderProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a MatchNoneQueryBuilder", queryBuilder instanceof MatchNoneQueryBuilder);
    }

    public void testParseInnerQueryBuilderProtoWithTerm() {
        // Create a QueryContainer with Term query
        // Create a FieldValue for the term value
        FieldValue fieldValue = FieldValue.newBuilder().setStringValue("test-value").build();

        // Create a TermQuery with the FieldValue and field name
        TermQuery termQuery = TermQuery.newBuilder().setField("test-field").setValue(fieldValue).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Call parseInnerQueryBuilderProto
        QueryBuilder queryBuilder = AbstractQueryBuilderProtoUtils.parseInnerQueryBuilderProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermQueryBuilder", queryBuilder instanceof TermQueryBuilder);
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test-field", termQueryBuilder.fieldName());
        assertEquals("Value should match", "test-value", termQueryBuilder.value());
    }

    public void testParseInnerQueryBuilderProtoWithTerms() {
        // Create a StringArray for terms values
        StringArray stringArray = StringArray.newBuilder().addStringArray("value1").addStringArray("value2").build();

        // Create a TermsLookupFieldStringArrayMap
        TermsLookupFieldStringArrayMap termsLookupFieldStringArrayMap = TermsLookupFieldStringArrayMap.newBuilder()
            .setStringArray(stringArray)
            .build();

        // Create a map for TermsLookupFieldStringArrayMap
        Map<String, TermsLookupFieldStringArrayMap> termsLookupFieldStringArrayMapMap = new HashMap<>();
        termsLookupFieldStringArrayMapMap.put("test-field", termsLookupFieldStringArrayMap);

        // Create a TermsQueryField
        TermsQueryField termsQueryField = TermsQueryField.newBuilder()
            .putAllTermsLookupFieldStringArrayMap(termsLookupFieldStringArrayMapMap)
            .build();

        // Create a QueryContainer with Terms query
        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQueryField).build();

        // Call parseInnerQueryBuilderProto
        QueryBuilder queryBuilder = AbstractQueryBuilderProtoUtils.parseInnerQueryBuilderProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermsQueryBuilder", queryBuilder instanceof TermsQueryBuilder);
        TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test-field", termsQueryBuilder.fieldName());
        assertEquals("Values size should match", 2, termsQueryBuilder.values().size());
        assertEquals("First value should match", "value1", termsQueryBuilder.values().get(0));
        assertEquals("Second value should match", "value2", termsQueryBuilder.values().get(1));
    }

    public void testParseInnerQueryBuilderProtoWithUnsupportedQueryType() {
        // Create an empty QueryContainer (no query type specified)
        QueryContainer queryContainer = QueryContainer.newBuilder().build();

        // Call parseInnerQueryBuilderProto, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> AbstractQueryBuilderProtoUtils.parseInnerQueryBuilderProto(queryContainer)
        );

        // Verify the exception message
        assertTrue("Exception message should mention 'Unsupported query type'", exception.getMessage().contains("Unsupported query type"));
    }
}
