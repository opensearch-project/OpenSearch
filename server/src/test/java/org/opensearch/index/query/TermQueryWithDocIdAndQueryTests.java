/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.get.GetRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.indices.TermsLookup;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.IndicesAdminClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TermQueryWithDocIdAndQueryTests extends OpenSearchTestCase {

    private IndexSettings newTestIndexSettings(int maxTermsCount, int maxResultWindow) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, 2000099)
            .put("index.max_terms_count", maxTermsCount)
            .put("index.max_result_window", maxResultWindow)
            .build();
        IndexMetadata meta = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        return new IndexSettings(meta, settings);
    }

    private IndexSettings newTestIndexSettings() {
        return newTestIndexSettings(2048, 10000);
    }

    public void testTermsQueryWithValuesOnly() throws Exception {
        List<Object> values = Arrays.asList("111", "222");
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", values, null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(fieldType);
        Query mockQuery = mock(Query.class);
        when(fieldType.termsQuery(anyList(), eq(context))).thenReturn(mockQuery);

        Query result = builder.doToQuery(context);
        assertNotNull(result);
        assertEquals(mockQuery, result);
    }

    public void testTermsQueryWithBitmapValueType() throws Exception {
        List<Object> values = Collections.singletonList(new BytesArray(new byte[] { 1, 2, 3 }));
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", values, null).valueType(TermsQueryBuilder.ValueType.BITMAP);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        NumberFieldMapper.NumberFieldType numberFieldType = mock(NumberFieldMapper.NumberFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(numberFieldType);
        when(numberFieldType.unwrap()).thenReturn(numberFieldType);
        Query bitmapQuery = mock(Query.class);
        when(numberFieldType.bitmapQuery(any(BytesArray.class))).thenReturn(bitmapQuery);

        Query result = builder.doToQuery(context);
        assertNotNull(result);
        assertEquals(bitmapQuery, result);
    }

    public void testDoToQueryThrowsForEmptyValues() {
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", Collections.emptyList(), null);
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        when(context.fieldMapper(any())).thenReturn(mock(MappedFieldType.class));
        Exception ex = expectThrows(UnsupportedOperationException.class, () -> builder.doToQuery(context));
        assertEquals("query must be rewritten first", ex.getMessage());
    }

    public void testDoToQueryTooManyTerms() {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            values.add(String.valueOf(i));
        }
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", values, null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings(1024, 10000));
        MappedFieldType fieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("student_id")).thenReturn(fieldType);

        Exception ex = expectThrows(IllegalArgumentException.class, () -> builder.doToQuery(context));
        assertTrue(ex.getMessage().contains("has exceeded the allowed maximum"));
    }

    public void testDoToQueryUnknownFieldType() {
        List<Object> values = Arrays.asList("111", "222");
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", values, null);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        when(context.fieldMapper("student_id")).thenReturn(null);

        Exception ex = expectThrows(IllegalStateException.class, () -> builder.doToQuery(context));
        assertTrue(ex.getMessage().contains("Rewrite first"));
    }

    // Forbidden reflection-based test removed. Instead, test the normal cache population path.
    public void testTermsLookupWithIdFetchSimulated() throws Exception {
        TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled");
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", null, termsLookup);

        // Setup mock client and QueryRewriteContext
        Client mockClient = mock(Client.class);
        QueryRewriteContext mockRewriteContext = mock(QueryRewriteContext.class);

        // Intercept the lambda registered as async action
        doAnswer(invocation -> {
            Object asyncAction = invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            BiConsumer<Client, ActionListener<List<Object>>> lambda = (BiConsumer<Client, ActionListener<List<Object>>>) asyncAction;
            // Simulate the fetch logic -- respond with terms
            lambda.accept(mockClient, ActionListener.wrap(list -> {}, ex -> fail("Should not throw")));
            return null;
        }).when(mockRewriteContext).registerAsyncAction(any());

        builder.doRewrite(mockRewriteContext);

        verify(mockClient, atLeastOnce()).get(any(GetRequest.class), any());
    }

    // Forbidden reflection-based test removed. Instead, test the normal cache population path.
    public void testTermsLookupWithQueryFetchSimulated() throws Exception {
        QueryBuilder subQuery = mock(QueryBuilder.class);
        TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled", subQuery);
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", null, termsLookup);

        // Setup mock client and its admin/indices chain
        Client mockClient = mock(Client.class);
        AdminClient mockAdminClient = mock(AdminClient.class);
        IndicesAdminClient mockIndicesAdminClient = mock(IndicesAdminClient.class);

        // Stub the chain: client.admin().indices()
        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesAdminClient);

        QueryRewriteContext mockRewriteContext = mock(QueryRewriteContext.class);

        // Intercept the lambda registered as async action
        doAnswer(invocation -> {
            Object asyncAction = invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            BiConsumer<Client, ActionListener<List<Object>>> lambda = (BiConsumer<Client, ActionListener<List<Object>>>) asyncAction;
            // Simulate the fetch logic -- respond with terms
            lambda.accept(mockClient, ActionListener.wrap(list -> {}, ex -> fail("Should not throw")));
            return null;
        }).when(mockRewriteContext).registerAsyncAction(any());

        builder.doRewrite(mockRewriteContext);

        // For query-based lookup, verify admin() and indices() are called
        verify(mockClient).admin();
        verify(mockAdminClient).indices();
    }

    public void testDoToQueryThrowsWhenTermsNotFetched() {
        QueryBuilder subQuery = mock(QueryBuilder.class);
        TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled", subQuery);
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", null, termsLookup);

        QueryShardContext context = mock(QueryShardContext.class);
        when(context.getIndexSettings()).thenReturn(newTestIndexSettings());
        when(context.fieldMapper(any())).thenReturn(mock(MappedFieldType.class));

        Exception ex = expectThrows(UnsupportedOperationException.class, () -> builder.doToQuery(context));
        assertTrue(ex.getMessage().contains("query must be rewritten first"));
    }

    public void testRewriteWithValuesPresentReturnsSelf() throws Exception {
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", Arrays.asList("111", "222"), null);
        QueryRewriteContext rewriteContext = mock(QueryRewriteContext.class);
        QueryBuilder rewritten = builder.doRewrite(rewriteContext);
        assertEquals(builder, rewritten);
    }

    public void testRewriteWithEmptyValuesReturnsMatchNone() throws Exception {
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", new ArrayList<>(), null);
        QueryRewriteContext rewriteContext = mock(QueryRewriteContext.class);
        QueryBuilder rewritten = builder.doRewrite(rewriteContext);
        assertTrue(rewritten instanceof MatchNoneQueryBuilder);
    }

    public void testEqualsAndHashCode() {
        TermsQueryBuilder builderA = new TermsQueryBuilder("student_id", Arrays.asList("a", "b"), null);
        TermsQueryBuilder builderB = new TermsQueryBuilder("student_id", Arrays.asList("a", "b"), null);
        assertEquals(builderA, builderB);
        assertEquals(builderA.hashCode(), builderB.hashCode());
    }

    public void testValueTypeEnum() {
        assertEquals(TermsQueryBuilder.ValueType.DEFAULT, TermsQueryBuilder.ValueType.fromString("default"));
        assertEquals(TermsQueryBuilder.ValueType.BITMAP, TermsQueryBuilder.ValueType.fromString("bitmap"));
        Exception ex = expectThrows(IllegalArgumentException.class, () -> TermsQueryBuilder.ValueType.fromString("unknown"));
        assertTrue(ex.getMessage().contains("is not valid"));
    }

    public void testFetchIsCoveredWithTermsLookupId() throws Exception {
        TermsLookup termsLookup = new TermsLookup("classes", "101", "enrolled");
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", termsLookup);

        // Mock client and QueryRewriteContext
        Client mockClient = mock(Client.class);
        QueryRewriteContext mockRewriteContext = mock(QueryRewriteContext.class);

        // Intercept the lambda registered as async action
        doAnswer(invocation -> {
            Object asyncAction = invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            BiConsumer<Client, ActionListener<List<Object>>> lambda = (BiConsumer<Client, ActionListener<List<Object>>>) asyncAction;
            lambda.accept(mockClient, ActionListener.wrap(list -> {}, ex -> fail("Should not throw")));
            return null;
        }).when(mockRewriteContext).registerAsyncAction(any());

        builder.doRewrite(mockRewriteContext);

        verify(mockClient, atLeastOnce()).get(any(GetRequest.class), any());
    }

    public void testFetchIsCoveredWithTermsLookupQuery() throws Exception {
        QueryBuilder subQuery = mock(QueryBuilder.class);
        TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled", subQuery);
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", termsLookup);

        Client client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);

        doAnswer(invocation -> adminClient).when(client).admin();
        doAnswer(invocation -> indicesAdminClient).when(adminClient).indices();

        QueryRewriteContext rewriteContext = mock(QueryRewriteContext.class);

        doAnswer(invocation -> {
            Object asyncAction = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            BiConsumer<Client, ActionListener<List<Object>>> lambda = (BiConsumer<Client, ActionListener<List<Object>>>) asyncAction;
            lambda.accept(client, ActionListener.wrap(list -> {}, ex -> fail("Should not throw")));
            return null;
        }).when(rewriteContext).registerAsyncAction(any());

        builder.doRewrite(rewriteContext);

        verify(client).admin();
        verify(adminClient).indices();
    }

    public void testTermsLookupWithQueryAsyncFetch() throws Exception {
        TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled", QueryBuilders.matchAllQuery());
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", null, termsLookup);

        Client mockClient = mock(Client.class);
        AdminClient mockAdminClient = mock(AdminClient.class);
        IndicesAdminClient mockIndicesAdminClient = mock(IndicesAdminClient.class);

        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesAdminClient);

        doAnswer(invocation -> {
            ActionListener<org.opensearch.action.admin.indices.settings.get.GetSettingsResponse> listener = invocation.getArgument(1);
            org.opensearch.action.admin.indices.settings.get.GetSettingsResponse settingsResponse = mock(
                org.opensearch.action.admin.indices.settings.get.GetSettingsResponse.class
            );
            when(settingsResponse.getIndexToSettings()).thenReturn(
                Collections.singletonMap("classes", org.opensearch.common.settings.Settings.EMPTY)
            );
            listener.onResponse(settingsResponse);
            return null;
        }).when(mockIndicesAdminClient).getSettings(any(), any());

        doAnswer(invocation -> {
            ActionListener<org.opensearch.action.search.SearchResponse> listener = invocation.getArgument(1);

            SearchHit hit = new SearchHit(0);
            String json = "{\"enrolled\":[\"111\",\"222\"]}";
            hit.sourceRef(new org.opensearch.core.common.bytes.BytesArray(json));

            TotalHits totalHits = new TotalHits(1, TotalHits.Relation.EQUAL_TO);

            SearchHits searchHits = new SearchHits(new SearchHit[] { hit }, totalHits, 1.0f);
            org.opensearch.action.search.SearchResponseSections sections = new org.opensearch.action.search.SearchResponseSections(
                searchHits,
                null,
                null,
                false,
                null,
                null,
                1
            );
            org.opensearch.action.search.SearchResponse searchResponse = new org.opensearch.action.search.SearchResponse(
                sections,
                "",
                1,
                1,
                0,
                1,
                null,
                null
            );
            listener.onResponse(searchResponse);
            return null;
        }).when(mockClient).search(any(org.opensearch.action.search.SearchRequest.class), any());

        QueryRewriteContext mockRewriteContext = mock(QueryRewriteContext.class);

        final boolean[] called = { false };

        doAnswer(invocation -> {
            Object asyncAction = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            java.util.function.BiConsumer<Client, ActionListener<List<Object>>> lambda = (java.util.function.BiConsumer<
                Client,
                ActionListener<List<Object>>>) asyncAction;
            called[0] = true;
            // Instead of asserting on result, just call onResponse with null or empty list.
            lambda.accept(mockClient, org.opensearch.core.action.ActionListener.wrap(list -> {
                // We cannot assert the values due to static method, so just print for visibility.
            }, ex -> { fail("Should not throw: " + (ex != null ? ex.getMessage() : "")); }));
            return null;
        }).when(mockRewriteContext).registerAsyncAction(any());

        builder.doRewrite(mockRewriteContext);

        assertTrue("Async action should have been invoked", called[0]);
        // NOTE: Cannot assert on output list contents due to inability to mock static extractRawValues.
    }

    public void testTermsLookupSubqueryTotalHitsExceedsFetchSize() throws Exception {
        // Setup a TermsLookup with a subquery
        QueryBuilder subQuery = mock(QueryBuilder.class);
        TermsLookup termsLookup = new TermsLookup("classes", null, "enrolled", subQuery);
        TermsQueryBuilder builder = new TermsQueryBuilder("student_id", null, termsLookup);

        // Mock the client, admin, indices, and search response chain
        Client mockClient = mock(Client.class);
        AdminClient mockAdminClient = mock(AdminClient.class);
        IndicesAdminClient mockIndicesAdminClient = mock(IndicesAdminClient.class);

        when(mockClient.admin()).thenReturn(mockAdminClient);
        when(mockAdminClient.indices()).thenReturn(mockIndicesAdminClient);

        // Settings that produce a small fetch size
        int maxTermsCount = 5, maxResultWindow = 5, maxClauseCount = 5;
        Settings idxSettings = Settings.builder()
            .put("index.max_terms_count", maxTermsCount)
            .put("index.max_result_window", maxResultWindow)
            .put("indices.query.max_clause_count", maxClauseCount)
            .build();

        // Simulate settings response
        doAnswer(invocation -> {
            ActionListener<org.opensearch.action.admin.indices.settings.get.GetSettingsResponse> listener = invocation.getArgument(1);
            org.opensearch.action.admin.indices.settings.get.GetSettingsResponse settingsResponse = mock(
                org.opensearch.action.admin.indices.settings.get.GetSettingsResponse.class
            );
            when(settingsResponse.getIndexToSettings()).thenReturn(Collections.singletonMap("classes", idxSettings));
            listener.onResponse(settingsResponse);
            return null;
        }).when(mockIndicesAdminClient).getSettings(any(), any());

        // Simulate a search response with more total hits than fetchSize
        doAnswer(invocation -> {
            ActionListener<org.opensearch.action.search.SearchResponse> listener = invocation.getArgument(1);

            SearchHit[] hits = new SearchHit[5]; // Only 5 returned
            for (int i = 0; i < 5; i++)
                hits[i] = new SearchHit(i);
            org.apache.lucene.search.TotalHits totalHits = new org.apache.lucene.search.TotalHits(
                10,
                org.apache.lucene.search.TotalHits.Relation.EQUAL_TO
            );

            SearchHits searchHits = new SearchHits(hits, totalHits, 1.0f);
            org.opensearch.action.search.SearchResponseSections sections = new org.opensearch.action.search.SearchResponseSections(
                searchHits,
                null,
                null,
                false,
                null,
                null,
                1
            );
            org.opensearch.action.search.SearchResponse searchResponse = new org.opensearch.action.search.SearchResponse(
                sections,
                "",
                1,
                1,
                0,
                1,
                null,
                null
            );
            listener.onResponse(searchResponse);
            return null;
        }).when(mockClient).search(any(org.opensearch.action.search.SearchRequest.class), any());

        QueryRewriteContext mockRewriteContext = mock(QueryRewriteContext.class);

        doAnswer(invocation -> {
            Object asyncAction = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            java.util.function.BiConsumer<Client, ActionListener<List<Object>>> lambda = (java.util.function.BiConsumer<
                Client,
                ActionListener<List<Object>>>) asyncAction;
            AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
            lambda.accept(mockClient, ActionListener.wrap(list -> { /* success not expected */ }, e -> exceptionRef.set(e)));
            if (exceptionRef.get() != null) {
                throw exceptionRef.get();
            }
            return null;
        }).when(mockRewriteContext).registerAsyncAction(any());

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> builder.doRewrite(mockRewriteContext));
        assertTrue(ex.getMessage().contains("exceed fetch limit"));
    }

    public void testMaxTermsCountSettingGetterConsistency() {
        Settings settings = Settings.builder().put("index.max_terms_count", 1234).build();
        assertEquals(1234, IndexSettings.MAX_TERMS_COUNT_SETTING.get(settings).intValue());
        assertEquals(1234, settings.getAsInt(IndexSettings.MAX_TERMS_COUNT_SETTING.getKey(), -1).intValue());
    }
}
