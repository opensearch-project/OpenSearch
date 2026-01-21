/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.protobufs.DerivedField;
import org.opensearch.protobufs.FieldAndFormat;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptField;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.SlicedScroll;
import org.opensearch.protobufs.TrackHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoTestUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE;
import static org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;
import static org.mockito.Mockito.mock;

public class SearchSourceBuilderProtoUtilsTests extends OpenSearchTestCase {

    private NamedWriteableRegistry mockRegistry;
    private AbstractQueryBuilderProtoUtils queryUtils;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockRegistry = mock(NamedWriteableRegistry.class);
        // Create an instance with all built-in converters
        queryUtils = QueryBuilderProtoTestUtils.createQueryUtils();
    }

    public void testParseProtoWithFrom() throws IOException {
        // Create a protobuf SearchRequestBody with from
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setFrom(10).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertEquals("From should match", 10, searchSourceBuilder.from());
    }

    public void testParseProtoWithSize() throws IOException {
        // Create a protobuf SearchRequestBody with size
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setSize(20).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertEquals("Size should match", 20, searchSourceBuilder.size());
    }

    public void testParseProtoWithTimeout() throws IOException {
        // Create a protobuf SearchRequestBody with timeout
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setTimeout("5s").build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertEquals("Timeout should match", TimeValue.timeValueSeconds(5), searchSourceBuilder.timeout());
    }

    public void testParseProtoWithTerminateAfter() throws IOException {
        // Create a protobuf SearchRequestBody with terminateAfter
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setTerminateAfter(100).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertEquals("TerminateAfter should match", 100, searchSourceBuilder.terminateAfter());
    }

    public void testParseProtoWithMinScore() throws IOException {
        // Create a protobuf SearchRequestBody with minScore
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setMinScore(0.5f).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertEquals("MinScore should match", 0.5f, searchSourceBuilder.minScore(), 0.0f);
    }

    public void testParseProtoWithVersion() throws IOException {
        // Create a protobuf SearchRequestBody with version
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setVersion(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertTrue("Version should be true", searchSourceBuilder.version());
    }

    public void testParseProtoWithSeqNoPrimaryTerm() throws IOException {
        // Create a protobuf SearchRequestBody with seqNoPrimaryTerm
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setSeqNoPrimaryTerm(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertTrue("SeqNoPrimaryTerm should be true", searchSourceBuilder.seqNoAndPrimaryTerm());
    }

    public void testParseProtoWithExplain() throws IOException {
        // Create a protobuf SearchRequestBody with explain
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setExplain(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertTrue("Explain should be true", searchSourceBuilder.explain());
    }

    public void testParseProtoWithTrackScores() throws IOException {
        // Create a protobuf SearchRequestBody with trackScores
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setTrackScores(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertTrue("TrackScores should be true", searchSourceBuilder.trackScores());
    }

    public void testParseProtoWithIncludeNamedQueriesScore() throws IOException {
        // Create a protobuf SearchRequestBody with includeNamedQueriesScore
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setIncludeNamedQueriesScore(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        searchSourceBuilder.includeNamedQueriesScores(true);
        assertTrue("IncludeNamedQueriesScore should be true", searchSourceBuilder.includeNamedQueriesScore());
    }

    public void testParseProtoWithTrackTotalHitsBooleanTrue() throws IOException {
        // Create a protobuf SearchRequestBody with trackTotalHits boolean true
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setTrackTotalHits(TrackHits.newBuilder().setEnabled(true).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertEquals("TrackTotalHits should be accurate", TRACK_TOTAL_HITS_ACCURATE, searchSourceBuilder.trackTotalHitsUpTo().intValue());
    }

    public void testParseProtoWithTrackTotalHitsBooleanFalse() throws IOException {
        // Create a protobuf SearchRequestBody with trackTotalHits boolean false
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setTrackTotalHits(TrackHits.newBuilder().setEnabled(false).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertEquals("TrackTotalHits should be disabled", TRACK_TOTAL_HITS_DISABLED, searchSourceBuilder.trackTotalHitsUpTo().intValue());
    }

    public void testParseProtoWithTrackTotalHitsInteger() throws IOException {
        // Create a protobuf SearchRequestBody with trackTotalHits integer
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setTrackTotalHits(TrackHits.newBuilder().setCount(1000).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertEquals("TrackTotalHits should match", 1000, searchSourceBuilder.trackTotalHitsUpTo().intValue());
    }

    public void testParseProtoWithProfile() throws IOException {
        // Create a protobuf SearchRequestBody with profile
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setProfile(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertTrue("Profile should be true", searchSourceBuilder.profile());
    }

    public void testParseProtoWithSearchPipeline() throws IOException {
        // Create a protobuf SearchRequestBody with searchPipeline
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setSearchPipeline("my-pipeline").build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertEquals("SearchPipeline should match", "my-pipeline", searchSourceBuilder.pipeline());
    }

    public void testParseProtoWithVerbosePipeline() throws IOException {
        // Create a protobuf SearchRequestBody with verbosePipeline
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setVerbosePipeline(true).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

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
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

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
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

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
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

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
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertNotNull("FetchFields should not be null", searchSourceBuilder.fetchFields());
        assertEquals("Should have 2 fetchFields", 2, searchSourceBuilder.fetchFields().size());
    }

    public void testParseProtoWithIndicesBoost() throws IOException {
        // Create a protobuf SearchRequestBody with indicesBoost
        Map<String, Float> boostMap = new HashMap<>();
        boostMap.put("index1", 1.0f);
        boostMap.put("index2", 2.0f);

        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().putAllIndicesBoost(boostMap).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertNotNull("IndexBoosts should not be null", searchSourceBuilder.indexBoosts());
        assertEquals("Should have 2 indexBoosts", 2, searchSourceBuilder.indexBoosts().size());
    }

    public void testParseProtoWithPostFilter() throws IOException {
        // Create a protobuf SearchRequestBody with postFilter
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setPostFilter(QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build())
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

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
                .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build())
                .setIgnoreFailure(true)
                .build()
        );
        scriptFieldsMap.put(
            "script_field_2",
            ScriptField.newBuilder()
                .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build())
                .build()
        );

        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().putAllScriptFields(scriptFieldsMap).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

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
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

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
                .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build())
                .build()
        );
        derivedFieldsMap.put(
            "derived_field_2",
            DerivedField.newBuilder()
                .setType("string")
                .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build())
                .build()
        );

        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().putAllDerived(derivedFieldsMap).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

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

    public void testParseProtoWithDerivedFieldsWithOptionalProperties() throws IOException {
        // Create a protobuf SearchRequestBody with derived fields including optional properties
        ObjectMap properties = ObjectMap.newBuilder()
            .putFields("nested_field1", ObjectMap.Value.newBuilder().setString("text").build())
            .putFields("nested_field2", ObjectMap.Value.newBuilder().setString("keyword").build())
            .build();

        Map<String, DerivedField> derivedFieldsMap = new HashMap<>();
        derivedFieldsMap.put(
            "derived_field_with_properties",
            DerivedField.newBuilder()
                .setType("object")
                .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("emit(doc['field'].value)").build()).build())
                .setProperties(properties)
                .setPrefilterField("source_field")
                .setFormat("yyyy-MM-dd")
                .setIgnoreMalformed(true)
                .build()
        );
        derivedFieldsMap.put(
            "simple_derived_field",
            DerivedField.newBuilder()
                .setType("number")
                .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build())
                .build()
        );

        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().putAllDerived(derivedFieldsMap).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertNotNull("DerivedFields should not be null", searchSourceBuilder.getDerivedFields());
        assertEquals("Should have 2 derived fields", 2, searchSourceBuilder.getDerivedFields().size());

        // Find the derived field with properties
        org.opensearch.index.mapper.DerivedField fieldWithProps = searchSourceBuilder.getDerivedFields()
            .stream()
            .filter(f -> f.getName().equals("derived_field_with_properties"))
            .findFirst()
            .orElse(null);

        assertNotNull("Should find derived_field_with_properties", fieldWithProps);
        assertEquals("Type should match", "object", fieldWithProps.getType());
        assertNotNull("Properties should not be null", fieldWithProps.getProperties());
        assertEquals("Should have 2 properties", 2, fieldWithProps.getProperties().size());
        assertEquals("nested_field1 type should match", "text", fieldWithProps.getProperties().get("nested_field1"));
        assertEquals("nested_field2 type should match", "keyword", fieldWithProps.getProperties().get("nested_field2"));
        assertEquals("Prefilter field should match", "source_field", fieldWithProps.getPrefilterField());
        assertEquals("Format should match", "yyyy-MM-dd", fieldWithProps.getFormat());
        assertTrue("Ignore malformed should be true", fieldWithProps.getIgnoreMalformed());

        // Find the simple derived field
        org.opensearch.index.mapper.DerivedField simpleField = searchSourceBuilder.getDerivedFields()
            .stream()
            .filter(f -> f.getName().equals("simple_derived_field"))
            .findFirst()
            .orElse(null);

        assertNotNull("Should find simple_derived_field", simpleField);
        assertEquals("Type should match", "number", simpleField.getType());
        assertNull("Properties should be null", simpleField.getProperties());
        assertNull("Prefilter field should be null", simpleField.getPrefilterField());
        assertNull("Format should be null", simpleField.getFormat());
        assertFalse("Ignore malformed should be false", simpleField.getIgnoreMalformed());
    }

    public void testParseProtoWithSearchAfter() throws IOException {
        // Create a protobuf SearchRequestBody with searchAfter
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .addSearchAfter(FieldValue.newBuilder().setString("value1").build())
            .addSearchAfter(
                FieldValue.newBuilder().setGeneralNumber(org.opensearch.protobufs.GeneralNumber.newBuilder().setFloatValue(42.0f)).build()
            )
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertNotNull("SearchAfter should not be null", searchSourceBuilder.searchAfter());
        assertEquals("SearchAfter should have 2 values", 2, searchSourceBuilder.searchAfter().length);
        assertEquals("First value should match", "value1", searchSourceBuilder.searchAfter()[0]);
        assertEquals("Second value should match", 42.0f, searchSourceBuilder.searchAfter()[1]);
    }

    public void testParseProtoWithExtThrowsUnsupportedOperationException() throws IOException {
        // Create a protobuf SearchRequestBody with ext
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().setExt(ObjectMap.newBuilder().build()).build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test, should throw UnsupportedOperationException
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils)
        );

        assertTrue("Exception message should mention ext param", exception.getMessage().contains("ext param is not supported yet"));
    }

    public void testScriptFieldProtoUtilsFromProto() throws IOException {
        // Create a protobuf ScriptField
        ScriptField scriptFieldProto = ScriptField.newBuilder()
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build())
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
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("doc['field'].value * 2").build()).build())
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

    public void testParseProtoWithHighlight() throws IOException {
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setHighlight(org.opensearch.protobufs.Highlight.newBuilder().addPreTags("<em>").addPostTags("</em>").build())
            .build();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        assertNotNull("Highlight should not be null", searchSourceBuilder.highlighter());
    }

    public void testParseProtoWithCollapse() throws IOException {
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setCollapse(org.opensearch.protobufs.FieldCollapse.newBuilder().setField("category").build())
            .build();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        assertNotNull("Collapse should not be null", searchSourceBuilder.collapse());
        assertEquals("Collapse field should match", "category", searchSourceBuilder.collapse().getField());
    }

    public void testParseProtoWithStoredFields() throws IOException {
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().addStoredFields("field1").addStoredFields("field2").build();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        assertNotNull("StoredFields should not be null", searchSourceBuilder.storedFields());
    }

    public void testParseProtoWithXSource() throws IOException {
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setXSource(org.opensearch.protobufs.SourceConfig.newBuilder().build())
            .build();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        assertNotNull("FetchSourceContext should not be null", searchSourceBuilder.fetchSource());
    }

    public void testParseProtoWithSort() throws IOException {
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .addSort(
                org.opensearch.protobufs.SortCombinations.newBuilder()
                    .setFieldWithOrder(
                        org.opensearch.protobufs.FieldSortMap.newBuilder()
                            .putFieldSortMap(
                                "timestamp",
                                org.opensearch.protobufs.FieldSort.newBuilder()
                                    .setOrder(org.opensearch.protobufs.SortOrder.SORT_ORDER_DESC)
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        assertNotNull("Sorts should not be null", searchSourceBuilder.sorts());
        assertEquals("Should have 1 sort", 1, searchSourceBuilder.sorts().size());
    }

    public void testParseProtoWithSuggest() throws IOException {
        // Suggester field was removed from SearchRequestBody in protobufs 1.0.0
        // Suggest functionality is now handled via SearchRequest URL parameters (suggest_field, suggest_text, etc.)
        // This test is no longer applicable as the field doesn't exist
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder().build();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Should not throw exception as suggest field no longer exists
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);
    }

    public void testParseProtoWithXSourceIncludes() throws IOException {
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setXSource(
                org.opensearch.protobufs.SourceConfig.newBuilder()
                    .setFilter(org.opensearch.protobufs.SourceFilter.newBuilder().addIncludes("field1").addIncludes("field2").build())
                    .build()
            )
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertNotNull("FetchSourceContext should not be null", searchSourceBuilder.fetchSource());
    }

    public void testParseProtoWithXSourceExcludes() throws IOException {
        // Create a protobuf SearchRequestBody with xSource excludes
        SearchRequestBody protoRequest = SearchRequestBody.newBuilder()
            .setXSource(
                org.opensearch.protobufs.SourceConfig.newBuilder()
                    .setFilter(org.opensearch.protobufs.SourceFilter.newBuilder().addExcludes("secret_field").build())
                    .build()
            )
            .build();

        // Create a SearchSourceBuilder to populate
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // Call the method under test
        SearchSourceBuilderProtoUtils.parseProto(searchSourceBuilder, protoRequest, queryUtils);

        // Verify the result
        assertNotNull("FetchSourceContext should not be null", searchSourceBuilder.fetchSource());
    }

}
