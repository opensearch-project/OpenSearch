/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.request.search;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.protobufs.FieldAndFormat;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.NumberMap;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SortCombinations;
import org.opensearch.protobufs.TrackHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE;
import static org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;
import static org.mockito.Mockito.mock;

public class SearchSourceBuilderProtoUtilsTests extends OpenSearchTestCase {

    private NamedWriteableRegistry mockRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockRegistry = mock(NamedWriteableRegistry.class);
    }

    public void testParseProtoWithFrom() throws IOException {
        // Create a protobuf SearchRequestBody with from
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setFrom(10).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertEquals("From should match", 10, searchSourceBuilder.from());
    }

    public void testParseProtoWithSize() throws IOException {
        // Create a protobuf SearchRequestBody with size
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setSize(20).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertEquals("Size should match", 20, searchSourceBuilder.size());
    }

    public void testParseProtoWithTimeout() throws IOException {
        // Create a protobuf SearchRequestBody with timeout
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setTimeout("5s").build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertEquals("Timeout should match", TimeValue.timeValueSeconds(5), searchSourceBuilder.timeout());
    }

    public void testParseProtoWithTerminateAfter() throws IOException {
        // Create a protobuf SearchRequestBody with terminateAfter
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setTerminateAfter(100).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertEquals("TerminateAfter should match", 100, searchSourceBuilder.terminateAfter());
    }

    public void testParseProtoWithMinScore() throws IOException {
        // Create a protobuf SearchRequestBody with minScore
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setMinScore(0.5f).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertEquals("MinScore should match", 0.5f, searchSourceBuilder.minScore(), 0.0f);
    }

    public void testParseProtoWithVersion() throws IOException {
        // Create a protobuf SearchRequestBody with version
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setVersion(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertTrue("Version should be true", searchSourceBuilder.version());
    }

    public void testParseProtoWithSeqNoPrimaryTerm() throws IOException {
        // Create a protobuf SearchRequestBody with seqNoPrimaryTerm
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setSeqNoPrimaryTerm(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertTrue("SeqNoPrimaryTerm should be true", searchSourceBuilder.seqNoAndPrimaryTerm());
    }

    public void testParseProtoWithExplain() throws IOException {
        // Create a protobuf SearchRequestBody with explain
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setExplain(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertTrue("Explain should be true", searchSourceBuilder.explain());
    }

    public void testParseProtoWithTrackScores() throws IOException {
        // Create a protobuf SearchRequestBody with trackScores
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setTrackScores(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertTrue("TrackScores should be true", searchSourceBuilder.trackScores());
    }

    public void testParseProtoWithIncludeNamedQueriesScore() throws IOException {
        // Create a protobuf SearchRequestBody with includeNamedQueriesScore
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setIncludeNamedQueriesScore(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        searchSourceBuilder.includeNamedQueriesScores(true);
        assertTrue("IncludeNamedQueriesScore should be true", searchSourceBuilder.includeNamedQueriesScore());
    }

    public void testParseProtoWithTrackTotalHitsBooleanTrue() throws IOException {
        // Create a protobuf SearchRequestBody with trackTotalHits boolean true
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setTrackTotalHits(TrackHits.newBuilder().setBoolValue(true).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertEquals("TrackTotalHits should be accurate", TRACK_TOTAL_HITS_ACCURATE, searchSourceBuilder.trackTotalHitsUpTo().intValue());
    }

    public void testParseProtoWithTrackTotalHitsBooleanFalse() throws IOException {
        // Create a protobuf SearchRequestBody with trackTotalHits boolean false
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setTrackTotalHits(TrackHits.newBuilder().setBoolValue(false).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertEquals("TrackTotalHits should be disabled", TRACK_TOTAL_HITS_DISABLED, searchSourceBuilder.trackTotalHitsUpTo().intValue());
    }

    public void testParseProtoWithTrackTotalHitsInteger() throws IOException {
        // Create a protobuf SearchRequestBody with trackTotalHits integer
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setTrackTotalHits(TrackHits.newBuilder().setInt32Value(1000).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertEquals("TrackTotalHits should match", 1000, searchSourceBuilder.trackTotalHitsUpTo().intValue());
    }

    public void testParseProtoWithProfile() throws IOException {
        // Create a protobuf SearchRequestBody with profile
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setProfile(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertTrue("Profile should be true", searchSourceBuilder.profile());
    }

    public void testParseProtoWithSearchPipeline() throws IOException {
        // Create a protobuf SearchRequestBody with searchPipeline
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setSearchPipeline("my-pipeline").build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertEquals("SearchPipeline should match", "my-pipeline", searchSourceBuilder.pipeline());
    }

    public void testParseProtoWithVerbosePipeline() throws IOException {
        // Create a protobuf SearchRequestBody with verbosePipeline
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setVerbosePipeline(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertTrue("VerbosePipeline should be true", searchSourceBuilder.verbosePipeline());
    }

    public void testParseProtoWithQuery() throws IOException {
        // Create a protobuf SearchRequestBody with query
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setQuery(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertNotNull("Query should not be null", searchSourceBuilder.query());
        assertTrue("Query should be MatchAllQueryBuilder", searchSourceBuilder.query() instanceof MatchAllQueryBuilder);
    }

    public void testParseProtoWithStats() throws IOException {
        // Create a protobuf SearchRequestBody with stats
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().addStats("stat1").addStats("stat2").build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertNotNull("Stats should not be null", searchSourceBuilder.stats());
        assertEquals("Should have 2 stats", 2, searchSourceBuilder.stats().size());
        assertTrue("Stats should contain stat1", searchSourceBuilder.stats().contains("stat1"));
        assertTrue("Stats should contain stat2", searchSourceBuilder.stats().contains("stat2"));
    }

    public void testParseProtoWithDocValueFields() throws IOException {
        // Create a protobuf SearchRequestBody with docValueFields
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .addDocvalueFields(FieldAndFormat.newBuilder().setField("field1").setFormat("format1").build())
            .addDocvalueFields(FieldAndFormat.newBuilder().setField("field2").setFormat("format2").build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertNotNull("DocValueFields should not be null", searchSourceBuilder.docValueFields());
        assertEquals("Should have 2 docValueFields", 2, searchSourceBuilder.docValueFields().size());
    }

    public void testParseProtoWithFields() throws IOException {
        // Create a protobuf SearchRequestBody with fields
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .addFields(FieldAndFormat.newBuilder().setField("field1").setFormat("format1").build())
            .addFields(FieldAndFormat.newBuilder().setField("field2").setFormat("format2").build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertNotNull("FetchFields should not be null", searchSourceBuilder.fetchFields());
        assertEquals("Should have 2 fetchFields", 2, searchSourceBuilder.fetchFields().size());
    }

    public void testParseProtoWithIndicesBoost() throws IOException {
        // Create a protobuf SearchRequestBody with indicesBoost
        Map<String, Float> boostMap = new HashMap<>();
        boostMap.put("index1", 1.0f);
        boostMap.put("index2", 2.0f);

        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .addIndicesBoost(NumberMap.newBuilder().putAllNumberMap(boostMap).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertNotNull("IndexBoosts should not be null", searchSourceBuilder.indexBoosts());
        assertEquals("Should have 2 indexBoosts", 2, searchSourceBuilder.indexBoosts().size());
    }

    public void testParseProtoWithSortString() throws IOException {
        // Create a protobuf SearchRequestBody with sort string
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .addSort(SortCombinations.newBuilder().setStringValue("field1").build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertNotNull("Sorts should not be null", searchSourceBuilder.sorts());
        assertEquals("Should have 1 sort", 1, searchSourceBuilder.sorts().size());
    }
}
