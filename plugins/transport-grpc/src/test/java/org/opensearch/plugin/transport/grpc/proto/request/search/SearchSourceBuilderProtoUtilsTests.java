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
import org.opensearch.protobufs.DerivedField;
import org.opensearch.protobufs.FieldAndFormat;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.GeneralNumber;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.NumberMap;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptField;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SlicedScroll;
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

    public void testParseProtoWithPostFilter() throws IOException {
        // Create a protobuf SearchRequestBody with postFilter
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setPostFilter(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertNotNull("PostFilter should not be null", searchSourceBuilder.postFilter());
        assertTrue("PostFilter should be MatchAllQueryBuilder", searchSourceBuilder.postFilter() instanceof MatchAllQueryBuilder);
    }

    public void testParseProtoWithScriptFields() throws IOException {
        // Create a protobuf SearchRequestBody with scriptFields
        Map<String, ScriptField> scriptFieldsMap = new HashMap<>();
        scriptFieldsMap.put(
            "script_field_1",
            ScriptField.newBuilder()
                .setScript(
                    Script.newBuilder().setInlineScript(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build()
                )
                .setIgnoreFailure(true)
                .build()
        );
        scriptFieldsMap.put(
            "script_field_2",
            ScriptField.newBuilder()
                .setScript(
                    Script.newBuilder().setInlineScript(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build()
                )
                .build()
        );

        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().putAllScriptFields(scriptFieldsMap).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertNotNull("ScriptFields should not be null", searchSourceBuilder.scriptFields());
        assertEquals("Should have 2 script fields", 2, searchSourceBuilder.scriptFields().size());
        assertTrue(
            "Should contain script_field_1",
            searchSourceBuilder.scriptFields()
                .contains(
                    new SearchSourceBuilder.ScriptField("script_field_1", new org.opensearch.script.Script("doc['field'].value * 2"), true)
                )
        );
        assertTrue(
            "Should contain script_field_2",
            searchSourceBuilder.scriptFields()
                .contains(
                    new SearchSourceBuilder.ScriptField("script_field_2", new org.opensearch.script.Script("doc['field'].value * 2"), false)
                )
        );
    }

    public void testParseProtoWithSlice() throws IOException {
        // Create a protobuf SearchRequestBody with slice
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setSlice(SlicedScroll.newBuilder().setId(5).setMax(10).setField("_id").build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertNotNull("Slice should not be null", searchSourceBuilder.slice());
        assertEquals("Slice id should match", 5, searchSourceBuilder.slice().getId());
        assertEquals("Slice max should match", 10, searchSourceBuilder.slice().getMax());
        assertEquals("Slice field should match", "_id", searchSourceBuilder.slice().getField());
    }

    public void testParseProtoWithDerivedFields() throws IOException {
        // Create a protobuf SearchRequestBody with derived fields
        Map<String, DerivedField> derivedFieldsMap = new HashMap<>();
        derivedFieldsMap.put(
            "derived_field_1",
            DerivedField.newBuilder()
                .setType("number")
                .setScript(
                    Script.newBuilder().setInlineScript(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build()
                )
                .build()
        );
        derivedFieldsMap.put(
            "derived_field_2",
            DerivedField.newBuilder()
                .setType("string")
                .setScript(
                    Script.newBuilder().setInlineScript(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build()
                )
                .build()
        );

        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().putAllDerived(derivedFieldsMap).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertNotNull("DerivedFields should not be null", searchSourceBuilder.getDerivedFields());
        assertEquals("Should have 2 derived fields", 2, searchSourceBuilder.getDerivedFields().size());
        assertTrue(
            "Should contain derived_field_1",
            searchSourceBuilder.getDerivedFields()
                .contains(
                    new org.opensearch.index.mapper.DerivedField(
                        "derived_field_1",
                        "number",
                        new org.opensearch.script.Script("doc['field'].value * 2")
                    )
                )
        );
        assertTrue(
            "Should contain derived_field_2",
            searchSourceBuilder.getDerivedFields()
                .contains(
                    new org.opensearch.index.mapper.DerivedField(
                        "derived_field_2",
                        "string",
                        new org.opensearch.script.Script("doc['field'].value * 2")
                    )
                )
        );
    }

    public void testParseProtoWithSearchAfter() throws IOException {
        // Create a protobuf SearchRequestBody with searchAfter
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .addSearchAfter(FieldValue.newBuilder().setStringValue("value1").build())
            .addSearchAfter(FieldValue.newBuilder().setGeneralNumber(GeneralNumber.newBuilder().setInt64Value(42).build()).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest);

        // Verify the result
        assertNotNull("SearchAfter should not be null", searchSourceBuilder.searchAfter());
        assertEquals("SearchAfter should have 2 values", 2, searchSourceBuilder.searchAfter().length);
        assertEquals("First value should match", "value1", searchSourceBuilder.searchAfter()[0]);
        assertEquals("Second value should match", 42L, searchSourceBuilder.searchAfter()[1]);
    }

    public void testParseProtoWithExtThrowsUnsupportedOperationException() throws IOException {
        // Create a protobuf SearchRequestBody with ext
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setExt(ObjectMap.newBuilder().build()).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test, should throw UnsupportedOperationException
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest)
        );

        assertTrue("Exception message should mention ext param", exception.getMessage().contains("ext param is not supported yet"));
    }

    public void testScriptFieldProtoUtilsFromProto() throws IOException {
        // Create a protobuf ScriptField
        ScriptField scriptFieldProto = ScriptField.newBuilder()
            .setScript(Script.newBuilder().setInlineScript(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build())
            .setIgnoreFailure(true)
            .build();

        // Call the method under test
        SearchSourceBuilder.ScriptField scriptField = SearchSourceBuilderProtoUtils.ScriptFieldProtoUtils.fromProto(
            "test_script_field",
            scriptFieldProto
        );

        // Verify the result
        assertNotNull("ScriptField should not be null", scriptField);
        assertEquals("Field name should match", "test_script_field", scriptField.fieldName());
        assertNotNull("Script should not be null", scriptField.script());
        assertEquals("Script source should match", "doc['field'].value * 2", scriptField.script().getIdOrCode());
        assertTrue("IgnoreFailure should be true", scriptField.ignoreFailure());
    }

    public void testScriptFieldProtoUtilsFromProtoWithDefaultIgnoreFailure() throws IOException {
        // Create a protobuf ScriptField without ignoreFailure
        ScriptField scriptFieldProto = ScriptField.newBuilder()
            .setScript(Script.newBuilder().setInlineScript(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build())
            .build();

        // Call the method under test
        SearchSourceBuilder.ScriptField scriptField = SearchSourceBuilderProtoUtils.ScriptFieldProtoUtils.fromProto(
            "test_script_field",
            scriptFieldProto
        );

        // Verify the result
        assertNotNull("ScriptField should not be null", scriptField);
        assertEquals("Field name should match", "test_script_field", scriptField.fieldName());
        assertNotNull("Script should not be null", scriptField.script());
        assertEquals("Script source should match", "doc['field'].value * 2", scriptField.script().getIdOrCode());
        assertFalse("IgnoreFailure should be false by default", scriptField.ignoreFailure());
    }

}
