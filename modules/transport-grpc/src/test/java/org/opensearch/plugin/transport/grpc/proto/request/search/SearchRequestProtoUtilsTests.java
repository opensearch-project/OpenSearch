/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.search;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugin.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.opensearch.plugin.transport.grpc.proto.request.search.query.QueryBuilderProtoTestUtils;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SourceConfigParam;
import org.opensearch.protobufs.TrackHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.search.suggest.term.TermSuggestionBuilder;
import org.opensearch.search.suggest.term.TermSuggestionBuilder.SuggestMode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class SearchRequestProtoUtilsTests extends OpenSearchTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;
    private Client mockClient;
    private AbstractQueryBuilderProtoUtils queryUtils;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        namedWriteableRegistry = mock(NamedWriteableRegistry.class);
        mockClient = mock(Client.class);
        queryUtils = QueryBuilderProtoTestUtils.createQueryUtils();
    }

    public void testParseSearchRequestWithBasicFields() throws IOException {
        // Create a protobuf SearchRequest with basic fields
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .addIndex("index1")
            .addIndex("index2")
            .setSearchType(org.opensearch.protobufs.SearchRequest.SearchType.SEARCH_TYPE_QUERY_THEN_FETCH)
            .setBatchedReduceSize(10)
            .setPreFilterShardSize(5)
            .setMaxConcurrentShardRequests(20)
            .setAllowPartialSearchResults(true)
            .setPhaseTook(true)
            .setRequestCache(true)
            .setScroll("1m")
            .addRouting("routing1")
            .addRouting("routing2")
            .setPreference("_local")
            .setSearchPipeline("pipeline1")
            .setCcsMinimizeRoundtrips(true)
            .setCancelAfterTimeInterval("30s")
            .build();

        // Create a SearchRequest to populate
        SearchRequest searchRequest = new SearchRequest();

        // Call the method under test
        SearchRequestProtoUtils.parseSearchRequest(searchRequest, protoRequest, namedWriteableRegistry, size -> {}, queryUtils);

        // Verify the result
        assertNotNull("SearchRequest should not be null", searchRequest);
        assertArrayEquals("Indices should match", new String[] { "index1", "index2" }, searchRequest.indices());
        assertEquals("SearchType should match", SearchType.QUERY_THEN_FETCH, searchRequest.searchType());
        assertEquals("BatchedReduceSize should match", 10, searchRequest.getBatchedReduceSize());
        assertEquals("PreFilterShardSize should match", 5, searchRequest.getPreFilterShardSize().intValue());
        assertEquals("MaxConcurrentShardRequests should match", 20, searchRequest.getMaxConcurrentShardRequests());
        assertTrue("AllowPartialSearchResults should be true", searchRequest.allowPartialSearchResults());
        assertTrue("PhaseTook should be true", searchRequest.isPhaseTook());
        assertTrue("RequestCache should be true", searchRequest.requestCache());
        assertNotNull("Scroll should not be null", searchRequest.scroll());
        assertEquals("Scroll timeout should match", TimeValue.timeValueMinutes(1), searchRequest.scroll().keepAlive());
        assertArrayEquals(
            "Routing should match",
            new String[] { "routing1", "routing2" },
            Strings.commaDelimitedListToStringArray(searchRequest.routing())
        );
        assertEquals("Preference should match", "_local", searchRequest.preference());
        assertEquals("SearchPipeline should match", "pipeline1", searchRequest.pipeline());
        assertTrue("CcsMinimizeRoundtrips should be true", searchRequest.isCcsMinimizeRoundtrips());
        assertEquals("CancelAfterTimeInterval should match", TimeValue.timeValueSeconds(30), searchRequest.getCancelAfterTimeInterval());
    }

    public void testParseSearchRequestWithRequestBody() throws IOException {
        // Create a protobuf SearchRequestBody
        SearchRequestBody requestBody = SearchRequestBody.newBuilder()
            .setFrom(10)
            .setSize(20)
            .setTimeout("5s")
            .setTerminateAfter(100)
            .setExplain(true)
            .setVersion(true)
            .setSeqNoPrimaryTerm(true)
            .setTrackScores(true)
            .setProfile(true)
            .build();

        // Create a protobuf SearchRequest with the request body
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .setRequestBody(requestBody)
            .build();

        // Create a SearchRequest to populate
        SearchRequest searchRequest = new SearchRequest();

        // Call the method under test
        SearchRequestProtoUtils.parseSearchRequest(searchRequest, protoRequest, namedWriteableRegistry, size -> {}, queryUtils);

        // Verify the result
        assertNotNull("SearchRequest should not be null", searchRequest);
        assertNotNull("Source should not be null", searchRequest.source());
        assertEquals("From should match", 10, searchRequest.source().from());
        assertEquals("Size should match", 20, searchRequest.source().size());
        assertEquals("Timeout should match", TimeValue.timeValueSeconds(5), searchRequest.source().timeout());
        assertEquals("TerminateAfter should match", 100, searchRequest.source().terminateAfter());
        assertTrue("Explain should be true", searchRequest.source().explain());
        assertTrue("Version should be true", searchRequest.source().version());
        assertTrue("SeqNoAndPrimaryTerm should be true", searchRequest.source().seqNoAndPrimaryTerm());
        assertTrue("TrackScores should be true", searchRequest.source().trackScores());
        assertTrue("Profile should be true", searchRequest.source().profile());
    }

    public void testParseSearchSourceWithQueryAndSort() throws IOException {
        // Create a protobuf SearchRequest with query and sort
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .setQ("field:value")
            .addSort(
                org.opensearch.protobufs.SearchRequest.SortOrder.newBuilder()
                    .setField("field1")
                    .setDirection(org.opensearch.protobufs.SearchRequest.SortOrder.Direction.DIRECTION_ASC)
                    .build()
            )
            .addSort(
                org.opensearch.protobufs.SearchRequest.SortOrder.newBuilder()
                    .setField("field2")
                    .setDirection(org.opensearch.protobufs.SearchRequest.SortOrder.Direction.DIRECTION_DESC)
                    .build()
            )
            .addSort(org.opensearch.protobufs.SearchRequest.SortOrder.newBuilder().setField("field3").build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchRequestProtoUtils.parseSearchSource(searchSourceBuilder, protoRequest, size -> {});

        // Verify the result
        assertNotNull("SearchSourceBuilder should not be null", searchSourceBuilder);
        assertNotNull("Query should not be null", searchSourceBuilder.query());
        assertNotNull("Sorts should not be null", searchSourceBuilder.sorts());
        assertEquals("Should have 3 sorts", 3, searchSourceBuilder.sorts().size());
    }

    public void testParseSearchSourceWithStoredFields() throws IOException {
        // Create a protobuf SearchRequest with stored fields
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .addStoredFields("field1")
            .addStoredFields("field2")
            .addStoredFields("field3")
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchRequestProtoUtils.parseSearchSource(searchSourceBuilder, protoRequest, size -> {});

        // Verify the result
        assertNotNull("SearchSourceBuilder should not be null", searchSourceBuilder);
        StoredFieldsContext storedFieldsContext = searchSourceBuilder.storedFields();
        assertNotNull("StoredFieldsContext should not be null", storedFieldsContext);
        assertEquals("Should have 3 stored fields", 3, storedFieldsContext.fieldNames().size());
        assertTrue("Should contain field1", storedFieldsContext.fieldNames().contains("field1"));
        assertTrue("Should contain field2", storedFieldsContext.fieldNames().contains("field2"));
        assertTrue("Should contain field3", storedFieldsContext.fieldNames().contains("field3"));
    }

    public void testParseSearchSourceWithDocValueFields() throws IOException {
        // Create a protobuf SearchRequest with doc value fields
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .addDocvalueFields("field1")
            .addDocvalueFields("field2")
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchRequestProtoUtils.parseSearchSource(searchSourceBuilder, protoRequest, size -> {});

        // Verify the result
        assertNotNull("SearchSourceBuilder should not be null", searchSourceBuilder);
        assertNotNull("DocValueFields should not be null", searchSourceBuilder.docValueFields());
        assertEquals("Should have 2 doc value fields", 2, searchSourceBuilder.docValueFields().size());
    }

    public void testParseSearchSourceWithSource() throws IOException {
        // Create a protobuf SearchRequest with source context
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .setSource(SourceConfigParam.newBuilder().setBoolValue(true).build())
            .addSourceIncludes("include1")
            .addSourceIncludes("include2")
            .addSourceExcludes("exclude1")
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchRequestProtoUtils.parseSearchSource(searchSourceBuilder, protoRequest, size -> {});

        // Verify the result
        assertNotNull("SearchSourceBuilder should not be null", searchSourceBuilder);
        org.opensearch.search.fetch.subphase.FetchSourceContext fetchSourceContext = searchSourceBuilder.fetchSource();
        assertNotNull("FetchSourceContext should not be null", fetchSourceContext);
        assertTrue("FetchSource should be true", fetchSourceContext.fetchSource());
        assertArrayEquals("Includes should match", new String[] { "include1", "include2" }, fetchSourceContext.includes());
        assertArrayEquals("Excludes should match", new String[] { "exclude1" }, fetchSourceContext.excludes());
    }

    public void testParseSearchSourceWithTrackTotalHitsBoolean() throws IOException {
        // Create a protobuf SearchRequest with track total hits boolean
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .setTrackTotalHits(TrackHits.newBuilder().setBoolValue(true).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchRequestProtoUtils.parseSearchSource(searchSourceBuilder, protoRequest, size -> {});

        // Verify the result
        assertNotNull("SearchSourceBuilder should not be null", searchSourceBuilder);
        assertTrue("TrackTotalHits should be true", searchSourceBuilder.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_ACCURATE);
    }

    public void testParseSearchSourceWithTrackTotalHitsInteger() throws IOException {
        // Create a protobuf SearchRequest with track total hits integer
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .setTrackTotalHits(TrackHits.newBuilder().setInt32Value(1000).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchRequestProtoUtils.parseSearchSource(searchSourceBuilder, protoRequest, size -> {});

        // Verify the result
        assertNotNull("SearchSourceBuilder should not be null", searchSourceBuilder);
        assertEquals("TrackTotalHitsUpTo should match", 1000, searchSourceBuilder.trackTotalHitsUpTo().intValue());
    }

    public void testParseSearchSourceWithStats() throws IOException {
        // Create a protobuf SearchRequest with stats
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .addStats("stat1")
            .addStats("stat2")
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchRequestProtoUtils.parseSearchSource(searchSourceBuilder, protoRequest, size -> {});

        // Verify the result
        assertNotNull("SearchSourceBuilder should not be null", searchSourceBuilder);
        assertNotNull("Stats should not be null", searchSourceBuilder.stats());
        assertEquals("Should have 2 stats", 2, searchSourceBuilder.stats().size());
        assertTrue("Should contain stat1", searchSourceBuilder.stats().contains("stat1"));
        assertTrue("Should contain stat2", searchSourceBuilder.stats().contains("stat2"));
    }

    public void testParseSearchSourceWithSuggest() throws IOException {
        // Create a protobuf SearchRequest with suggest
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .setSuggestField("title")
            .setSuggestText("opensearch")
            .setSuggestSize(10)
            .setSuggestMode(org.opensearch.protobufs.SearchRequest.SuggestMode.SUGGEST_MODE_POPULAR)
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchRequestProtoUtils.parseSearchSource(searchSourceBuilder, protoRequest, size -> {});

        // Verify the result
        assertNotNull("SearchSourceBuilder should not be null", searchSourceBuilder);
        SuggestBuilder suggestBuilder = searchSourceBuilder.suggest();
        assertNotNull("SuggestBuilder should not be null", suggestBuilder);
        assertEquals("Should have 1 suggestion", 1, suggestBuilder.getSuggestions().size());
        assertTrue("Should contain title suggestion", suggestBuilder.getSuggestions().containsKey("title"));
        assertEquals("SuggestText should match", "opensearch", suggestBuilder.getSuggestions().get("title").text());
        assertEquals("SuggestSize should match", 10, suggestBuilder.getSuggestions().get("title").size().intValue());
        assertEquals(
            "SuggestMode should match",
            SuggestMode.POPULAR,
            ((TermSuggestionBuilder) (suggestBuilder.getSuggestions().get("title"))).suggestMode()
        );
    }

    public void testCheckProtoTotalHitsWithRestTotalHitsAsInt() throws IOException {
        // Create a protobuf SearchRequest with rest_total_hits_as_int
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .setRestTotalHitsAsInt(true)
            .build();

        // Create a SearchRequest to populate
        SearchRequest searchRequest = new SearchRequest();

        // Call the method under test
        SearchRequestProtoUtils.checkProtoTotalHits(protoRequest, searchRequest);

        // Verify the result
        assertNotNull("SearchRequest should not be null", searchRequest);
        assertNotNull("Source should not be null", searchRequest.source());
        assertTrue("TrackTotalHits should be true", searchRequest.source().trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_ACCURATE);
    }

    public void testCheckProtoTotalHitsWithTrackTotalHitsUpTo() throws IOException {
        // Create a protobuf SearchRequest with rest_total_hits_as_int and track_total_hits_up_to
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .setRestTotalHitsAsInt(true)
            .build();

        // Create a SearchRequest with track_total_hits_up_to
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().trackTotalHitsUpTo(SearchContext.TRACK_TOTAL_HITS_ACCURATE));

        // Call the method under test
        SearchRequestProtoUtils.checkProtoTotalHits(protoRequest, searchRequest);

        // Verify the result
        assertNotNull("SearchRequest should not be null", searchRequest);
        assertNotNull("Source should not be null", searchRequest.source());
        assertEquals(
            "TrackTotalHitsUpTo should be ACCURATE",
            SearchContext.TRACK_TOTAL_HITS_ACCURATE,
            searchRequest.source().trackTotalHitsUpTo().intValue()
        );
    }

    public void testCheckProtoTotalHitsWithInvalidTrackTotalHitsUpTo() throws IOException {
        // Create a protobuf SearchRequest with rest_total_hits_as_int
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .setRestTotalHitsAsInt(true)
            .build();

        // Create a SearchRequest with invalid track_total_hits_up_to
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().trackTotalHitsUpTo(1000));

        // Call the method under test, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SearchRequestProtoUtils.checkProtoTotalHits(protoRequest, searchRequest)
        );

        assertTrue("Exception message should mention rest_total_hits_as_int", exception.getMessage().contains("rest_total_hits_as_int"));
    }

    public void testParseSearchSourceWithInvalidTerminateAfter() throws IOException {
        // Create a protobuf SearchRequest with invalid terminateAfter
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .setTerminateAfter(-1)
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SearchRequestProtoUtils.parseSearchSource(searchSourceBuilder, protoRequest, size -> {})
        );

        assertTrue(
            "Exception message should mention terminateAfter must be > 0",
            exception.getMessage().contains("terminateAfter must be > 0")
        );
    }

    public void testParseSearchSourceWithInvalidSortDirection() throws IOException {
        // Create a protobuf SearchRequest with invalid sort direction
        org.opensearch.protobufs.SearchRequest protoRequest = org.opensearch.protobufs.SearchRequest.newBuilder()
            .addSort(
                org.opensearch.protobufs.SearchRequest.SortOrder.newBuilder()
                    .setField("field1")
                    .setDirection(org.opensearch.protobufs.SearchRequest.SortOrder.Direction.DIRECTION_UNSPECIFIED)
                    .build()
            )
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SearchRequestProtoUtils.parseSearchSource(searchSourceBuilder, protoRequest, size -> {})
        );

        assertTrue(
            "Exception message should mention unsupported sort direction",
            exception.getMessage().contains("Unsupported sort direction")
        );
    }
}
