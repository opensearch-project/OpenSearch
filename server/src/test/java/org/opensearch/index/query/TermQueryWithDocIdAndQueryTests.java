/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.indices.TermsLookup;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class TermQueryWithDocIdAndQueryTests extends OpenSearchTestCase {

    // Utility to create a real IndexSettings for test use
    private static IndexSettings newTestIndexSettings(int maxTermsCount) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, 2000099)
            .put("index.max_terms_count", maxTermsCount)
            .build();
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return new IndexSettings(meta, settings);
    }

    private static IndexSettings newTestIndexSettings() {
        return newTestIndexSettings(2048);
    }

    public void testTermsQueryWithIdOnlyAndVerifyResults() throws Exception {
        TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled");
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", Arrays.asList("111", "222"), null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(fieldType);
        when(fieldType.termsQuery(anyList(), eq(context))).thenReturn(mock(Query.class));

        Query query = builder.doToQuery(context);
        assertNotNull(query);
    }

    public void testTermsQueryWithQueryOnlyAndVerifyResults() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "CS101");
        TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled", queryBuilder);
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", Arrays.asList("333", "444"), null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(fieldType);
        when(fieldType.termsQuery(anyList(), eq(context))).thenReturn(mock(Query.class));

        Query query = builder.doToQuery(context);
        assertNotNull(query);
    }

    public void testEnhancedTermsLookupWithQueryClause() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("course", "Math101");
        TermsLookup termsLookup = new TermsLookup("courses", null, "students", queryBuilder);
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", Arrays.asList("111", "555"), null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(fieldType);
        when(fieldType.termsQuery(anyList(), eq(context))).thenReturn(mock(Query.class));

        Query query = builder.doToQuery(context);
        assertNotNull(query);
    }

    public void testQueryClauseReturnsNoResults() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("course", "NonExistentCourse");
        TermsLookup termsLookup = new TermsLookup("courses", null, "students", queryBuilder);
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", new ArrayList<>(), null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(fieldType);

        Exception ex = expectThrows(UnsupportedOperationException.class, () -> builder.doToQuery(context));
        assertTrue(ex.getMessage().toLowerCase().contains("query must be rewritten first"));
    }

    public void testTermsQueryWithInsertedData() throws Exception {
        TermsLookup termsLookup = new TermsLookup("classes", "102", "enrolled");
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", Arrays.asList("333", "444"), null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(fieldType);
        when(fieldType.termsQuery(anyList(), eq(context))).thenReturn(mock(Query.class));

        Query query = builder.doToQuery(context);
        assertNotNull(query);
    }

    public void testTermsQueryWithNoIdAndNoQuery() {
        Exception exception = expectThrows(IllegalArgumentException.class, () -> { new TermsLookup("classes", null, "enrolled"); });
        assertEquals(
            "[" + TermsQueryBuilder.NAME + "] query lookup element requires specifying either the id or the query.",
            exception.getMessage()
        );
    }

    public void testTermsQueryWithIdAndQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("course", "CS102");
        TermsLookup termsLookup = new TermsLookup("classes", "103", "enrolled");

        Exception exception = expectThrows(IllegalArgumentException.class, () -> termsLookup.setQuery(queryBuilder));
        assertEquals(
            "[" + TermsQueryBuilder.NAME + "] query lookup element cannot specify both id and query.",
            exception.getMessage()
        );
    }

    public void testTermsQueryWithComplexQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("course", "CS103"))
            .filter(QueryBuilders.rangeQuery("year").gte(2020));
        TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled", queryBuilder);
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", Arrays.asList("555", "666"), null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(fieldType);
        when(fieldType.termsQuery(anyList(), eq(context))).thenReturn(mock(Query.class));

        Query query = builder.doToQuery(context);
        assertNotNull(query);
    }

    public void testRewriteWithTermsLookupQuery() throws IOException {
        QueryBuilder spyQuery = mock(QueryBuilder.class);

        TermsLookup termsLookup = new TermsLookup("test-index", null, "field", spyQuery);

        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("field", termsLookup);
        termsQueryBuilder.boost(1.0f);

        TermsQueryBuilder rewrittenQuery = new TermsQueryBuilder("field", Arrays.asList("value1", "value2"), null);

        QueryBuilder result = rewrittenQuery;

        assertTrue(result instanceof TermsQueryBuilder);
        TermsQueryBuilder resultTermsQuery = (TermsQueryBuilder) result;

        assertEquals("field", resultTermsQuery.fieldName());
        assertEquals(Arrays.asList("value1", "value2"), resultTermsQuery.values());
        assertEquals(1.0f, resultTermsQuery.boost(), 0.001f);

        String expectedJson = "{"
            + "\"terms\":{"
            + "\"field\":[\"value1\",\"value2\"],"
            + "\"boost\":1.0"
            + "}"
            + "}";

        XContentParser expectedParser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, expectedJson);
        Map<String, Object> expectedMap = expectedParser.map();

        String actualJson = result.toString();
        XContentParser actualParser = XContentType.JSON.xContent()
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, actualJson);
        Map<String, Object> actualMap = actualParser.map();

        assertEquals(expectedMap, actualMap);
    }

    public void testTermsLookupWithQueryDoToQueryPath() throws IOException {
        QueryShardContext context = mock(QueryShardContext.class);

        Client client = mock(Client.class);
        when(context.getClient()).thenReturn(client);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());

        SearchResponse mockResponse = mock(SearchResponse.class);
        SearchHits mockHits = new SearchHits(
            new SearchHit[] {
                new SearchHit(1).sourceRef(new BytesArray("{\"path\":\"value1\"}")),
                new SearchHit(2).sourceRef(new BytesArray("{\"path\":\"value2\"}")) },
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            1.0f
        );
        when(mockResponse.getHits()).thenReturn(mockHits);
        when(client.search(any(SearchRequest.class))).thenAnswer(invocation -> {
            ActionFuture<SearchResponse> actionFuture = mock(ActionFuture.class);
            when(actionFuture.actionGet()).thenReturn(mockResponse);
            return actionFuture;
        });

        MappedFieldType fieldMapper = mock(MappedFieldType.class);
        when(context.fieldMapper("fieldName")).thenReturn(fieldMapper);
        when(fieldMapper.termsQuery(anyList(), eq(context))).thenReturn(mock(Query.class));

        QueryBuilder queryBuilder = mock(QueryBuilder.class);
        TermsLookup termsLookup = new TermsLookup("index", null, "path", queryBuilder);

        QueryBuilder rewrittenQuery = mock(QueryBuilder.class);
        when(queryBuilder.rewrite(context)).thenReturn(rewrittenQuery);

        Query result = (Query) new Object() {
            public Query execute() throws IOException {
                if (termsLookup != null && termsLookup.query() != null) {
                    QueryBuilder rewrittenQuery = termsLookup.query().rewrite(context);

                    SearchResponse response = context.getClient()
                        .search(
                            new SearchRequest(termsLookup.index()).source(new SearchSourceBuilder().query(rewrittenQuery).fetchSource(true))
                        )
                        .actionGet();

                    List<Object> terms = new ArrayList<>();
                    for (SearchHit hit : response.getHits().getHits()) {
                        terms.addAll(XContentMapValues.extractRawValues(termsLookup.path(), hit.getSourceAsMap()));
                    }
                    return context.fieldMapper("fieldName").termsQuery(terms, context);
                }
                return null;
            }
        }.execute();

        assertNotNull(result);
        verify(client).search(any(SearchRequest.class));
        verify(context).fieldMapper("fieldName");
    }

    public void testDoToQueryThrowsWhenTermsNotFetched() {
        TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled", QueryBuilders.matchQuery("foo", "bar"));
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", termsLookup);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        when(context.fieldMapper(any())).thenReturn(mock(MappedFieldType.class));

        Exception exception = expectThrows(IllegalStateException.class, () -> {
            builder.doToQuery(context);
        });
        assertTrue(exception.getMessage().contains("Terms must be fetched during rewrite phase before query execution."));
    }

    public void testDoToQueryBitmapPath() throws Exception {
        List<Object> values = Arrays.asList(new BytesArray(new byte[]{ 1, 2, 3 }));
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", values, null);
        builder.valueType(TermsQueryBuilder.ValueType.BITMAP);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());

        // Use a real class type for the field type
        NumberFieldMapper.NumberFieldType numberFieldType = mock(NumberFieldMapper.NumberFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(numberFieldType);
        when(numberFieldType.unwrap()).thenReturn(numberFieldType);
        when(numberFieldType.bitmapQuery(any(BytesArray.class))).thenReturn(mock(Query.class));

        Query result = builder.doToQuery(context);
        assertNotNull(result);
    }

    public void testDoToQueryThrowsForNullOrEmptyValues() throws IOException {
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", (List<?>) null, null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        when(context.fieldMapper(any())).thenReturn(mock(MappedFieldType.class));

        Query query = builder.doToQuery(context);
        assertNull(query);
    }



    public void testDoToQueryUnknownFieldType() throws Exception {
        List<Object> values = Arrays.asList("111", "222");
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", values, null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        when(context.fieldMapper("student_id")).thenReturn(null);

        Exception exception = expectThrows(IllegalStateException.class, () -> {
            builder.doToQuery(context);
        });
        assertTrue(exception.getMessage().contains("Rewrite first"));
    }

    public void testDoToQueryTooManyTerms() throws Exception {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 11000; i++) {
            values.add(String.valueOf(i));
        }
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", values, null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings(1024)); // set maxTermsCount low

        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(fieldType);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> {
            builder.doToQuery(context);
        });
        assertTrue(exception.getMessage().contains("has exceeded the allowed maximum"));
    }

    public void testDoToQueryNormalTermsQuery() throws Exception {
        List<Object> values = Arrays.asList("111", "222");
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", values, null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings(2048));

        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(fieldType);
        when(fieldType.termsQuery(anyList(), eq(context))).thenReturn(mock(Query.class));

        Query result = builder.doToQuery(context);
        assertNotNull(result);
    }
}
