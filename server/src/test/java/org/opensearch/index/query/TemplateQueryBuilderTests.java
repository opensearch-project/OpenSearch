/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;

import static org.opensearch.index.query.TemplateQueryBuilder.NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TemplateQueryBuilderTests extends OpenSearchTestCase {

    /**
     * Tests the fromXContent method of TemplateQueryBuilder.
     * Verifies that a TemplateQueryBuilder can be correctly created from XContent.
     */
    public void testFromXContent() throws IOException {
        /*
            {
              "template": {
                "term": {
                  "message": {
                    "value": "foo"
                  }
                }
              }
            }
        */
        Map<String, Object> template = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        message.put("value", "foo");
        term.put("message", message);
        template.put("term", term);

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().map(template);

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        TemplateQueryBuilder templateQueryBuilder = TemplateQueryBuilder.fromXContent(contentParser);

        assertEquals(NAME, templateQueryBuilder.getWriteableName());
        assertEquals(template, templateQueryBuilder.getContent());

        SearchSourceBuilder source = new SearchSourceBuilder().query(templateQueryBuilder);
        assertEquals(source.toString(), "{\"query\":{\"template\":{\"term\":{\"message\":{\"value\":\"foo\"}}}}}");
    }

    /**
     * Tests the query source generation of TemplateQueryBuilder.
     * Verifies that the correct query source is generated from a TemplateQueryBuilder.
     */
    public void testQuerySource() {

        Map<String, Object> template = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        message.put("value", "foo");
        term.put("message", message);
        template.put("term", term);
        QueryBuilder incomingQuery = new TemplateQueryBuilder(template);
        SearchSourceBuilder source = new SearchSourceBuilder().query(incomingQuery);
        assertEquals(source.toString(), "{\"query\":{\"template\":{\"term\":{\"message\":{\"value\":\"foo\"}}}}}");
    }

    /**
     * Tests parsing a TemplateQueryBuilder from a JSON string.
     * Verifies that the parsed query matches the expected structure and can be serialized and deserialized.
     */
    public void testFromJson() throws IOException {
        String jsonString = "{\n"
            + "    \"geo_shape\": {\n"
            + "      \"location\": {\n"
            + "        \"shape\": {\n"
            + "          \"type\": \"Envelope\",\n"
            + "          \"coordinates\": \"${modelPredictionOutcome}\"\n"
            + "        },\n"
            + "        \"relation\": \"intersects\"\n"
            + "      },\n"
            + "      \"ignore_unmapped\": false,\n"
            + "      \"boost\": 42.0\n"
            + "    }\n"
            + "}";

        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, jsonString);
        parser.nextToken();
        TemplateQueryBuilder parsed = TemplateQueryBuilder.fromXContent(parser);

        // Check if the parsed query is an instance of TemplateQueryBuilder
        assertNotNull(parsed);
        assertTrue(parsed instanceof TemplateQueryBuilder);

        // Check if the content of the parsed query matches the expected content
        Map<String, Object> expectedContent = new HashMap<>();
        Map<String, Object> geoShape = new HashMap<>();
        Map<String, Object> location = new HashMap<>();
        Map<String, Object> shape = new HashMap<>();

        shape.put("type", "Envelope");
        shape.put("coordinates", "${modelPredictionOutcome}");
        location.put("shape", shape);
        location.put("relation", "intersects");
        geoShape.put("location", location);
        geoShape.put("ignore_unmapped", false);
        geoShape.put("boost", 42.0);
        expectedContent.put("geo_shape", geoShape);

        Map<String, Object> actualContent = new HashMap<>();
        actualContent.put("template", expectedContent);
        assertEquals(expectedContent, parsed.getContent());

        // Test that the query can be serialized and deserialized
        BytesStreamOutput out = new BytesStreamOutput();
        parsed.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        TemplateQueryBuilder deserializedQuery = new TemplateQueryBuilder(in);
        assertEquals(parsed.getContent(), deserializedQuery.getContent());

        // Test that the query can be converted to XContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        parsed.doXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> expectedJson = new HashMap<>();
        Map<String, Object> template = new HashMap<>();
        template.put("geo_shape", geoShape);
        expectedJson.put("template", template);

        XContentParser jsonParser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, builder.toString());
        Map<String, Object> actualJson = jsonParser.map();

        assertEquals(expectedJson, actualJson);
    }

    /**
     * Tests the constructor and getter methods of TemplateQueryBuilder.
     * Verifies that the content and writeable name are correctly set and retrieved.
     */
    public void testConstructorAndGetters() {
        Map<String, Object> content = new HashMap<>();
        content.put("key", "value");
        TemplateQueryBuilder builder = new TemplateQueryBuilder(content);

        assertEquals(content, builder.getContent());
        assertEquals(NAME, builder.getWriteableName());
    }

    /**
     * Tests the equals and hashCode methods of TemplateQueryBuilder.
     * Verifies that two builders with the same content are equal and have the same hash code,
     * while builders with different content are not equal and have different hash codes.
     */
    public void testEqualsAndHashCode() {
        Map<String, Object> content1 = new HashMap<>();
        content1.put("key", "value");
        TemplateQueryBuilder builder1 = new TemplateQueryBuilder(content1);

        Map<String, Object> content2 = new HashMap<>();
        content2.put("key", "value");
        TemplateQueryBuilder builder2 = new TemplateQueryBuilder(content2);

        Map<String, Object> content3 = new HashMap<>();
        content3.put("key", "different_value");
        TemplateQueryBuilder builder3 = new TemplateQueryBuilder(content3);

        assertTrue(builder1.equals(builder2));
        assertTrue(builder1.hashCode() == builder2.hashCode());
        assertFalse(builder1.equals(builder3));
        assertFalse(builder1.hashCode() == builder3.hashCode());
    }

    /**
     * Tests the doToQuery method of TemplateQueryBuilder.
     * Verifies that calling doToQuery throws an IllegalStateException.
     */
    public void testDoToQuery() {
        Map<String, Object> content = new HashMap<>();
        content.put("key", "value");
        TemplateQueryBuilder builder = new TemplateQueryBuilder(content);

        QueryShardContext mockContext = mock(QueryShardContext.class);
        expectThrows(IllegalStateException.class, () -> builder.doToQuery(mockContext));
    }

    /**
     * Tests the serialization and deserialization of TemplateQueryBuilder.
     * Verifies that a builder can be written to a stream and read back correctly.
     */
    public void testStreamRoundTrip() throws IOException {
        Map<String, Object> content = new HashMap<>();
        content.put("key", "value");
        TemplateQueryBuilder original = new TemplateQueryBuilder(content);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        TemplateQueryBuilder deserialized = new TemplateQueryBuilder(in);

        assertEquals(original, deserialized);
    }

    /**
     * Tests the doRewrite method of TemplateQueryBuilder with a simple term query.
     * Verifies that the template is correctly rewritten to a TermQueryBuilder.
     */
    public void testDoRewrite() throws IOException {

        Map<String, Object> template = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        message.put("value", "foo");
        term.put("message", message);
        template.put("term", term);
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("message", "foo");

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();

        Map<String, Object> contextVariables = new HashMap<>();
        when(queryRewriteContext.getContextVariables()).thenReturn(contextVariables);

        TermQueryBuilder newQuery = (TermQueryBuilder) templateQueryBuilder.doRewrite(queryRewriteContext);

        assertEquals(newQuery, termQueryBuilder);
        assertEquals(
            "{\n"
                + "  \"term\" : {\n"
                + "    \"message\" : {\n"
                + "      \"value\" : \"foo\",\n"
                + "      \"boost\" : 1.0\n"
                + "    }\n"
                + "  }\n"
                + "}",
            newQuery.toString()
        );
    }

    /**
     * Tests the doRewrite method of TemplateQueryBuilder with a string variable.
     * Verifies that the template is correctly rewritten with the variable substituted.
     */
    public void testDoRewriteWithString() throws IOException {

        Map<String, Object> template = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        message.put("value", "${response}");
        term.put("message", message);
        template.put("term", term);
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("message", "foo");

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();

        Map<String, Object> contextVariables = new HashMap<>();
        contextVariables.put("response", "foo");
        when(queryRewriteContext.getContextVariables()).thenReturn(contextVariables);

        TermQueryBuilder newQuery = (TermQueryBuilder) templateQueryBuilder.doRewrite(queryRewriteContext);

        assertEquals(newQuery, termQueryBuilder);
        assertEquals(
            "{\n"
                + "  \"term\" : {\n"
                + "    \"message\" : {\n"
                + "      \"value\" : \"foo\",\n"
                + "      \"boost\" : 1.0\n"
                + "    }\n"
                + "  }\n"
                + "}",
            newQuery.toString()
        );
    }

    /**
     * Tests the doRewrite method of TemplateQueryBuilder with a list variable.
     * Verifies that the template is correctly rewritten with the list variable substituted.
     */
    public void testDoRewriteWithList() throws IOException {
        ArrayList termsList = new ArrayList<>();
        termsList.add("foo");
        termsList.add("bar");

        Map<String, Object> template = new HashMap<>();
        Map<String, Object> terms = new HashMap<>();

        terms.put("message", "${response}");
        template.put("terms", terms);
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("message", termsList);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();

        Map<String, Object> contextVariables = new HashMap<>();
        contextVariables.put("response", termsList);
        when(queryRewriteContext.getContextVariables()).thenReturn(contextVariables);
        NamedXContentRegistry TEST_XCONTENT_REGISTRY_FOR_QUERY = new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
        );
        when(queryRewriteContext.getXContentRegistry()).thenReturn(TEST_XCONTENT_REGISTRY_FOR_QUERY);
        TermsQueryBuilder newQuery = (TermsQueryBuilder) templateQueryBuilder.doRewrite(queryRewriteContext);
        assertEquals(newQuery, termsQueryBuilder);
        assertEquals(
            "{\n"
                + "  \"terms\" : {\n"
                + "    \"message\" : [\n"
                + "      \"foo\",\n"
                + "      \"bar\"\n"
                + "    ],\n"
                + "    \"boost\" : 1.0\n"
                + "  }\n"
                + "}",
            newQuery.toString()
        );
    }

    /**
     * Tests the doRewrite method of TemplateQueryBuilder with a geo_distance query.
     * Verifies that the template is correctly rewritten for a geo_distance query.
     */
    public void testDoRewriteWithGeoDistanceQuery() throws IOException {
        Map<String, Object> template = new HashMap<>();
        Map<String, Object> geoDistance = new HashMap<>();

        geoDistance.put("distance", "12km");
        geoDistance.put("pin.location", "${geoPoint}");
        template.put("geo_distance", geoDistance);

        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        GeoPoint geoPoint = new GeoPoint(40, -70);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();
        Map<String, Object> contextVariables = new HashMap<>();
        contextVariables.put("geoPoint", geoPoint);
        when(queryRewriteContext.getContextVariables()).thenReturn(contextVariables);

        GeoDistanceQueryBuilder expectedQuery = new GeoDistanceQueryBuilder("pin.location");
        expectedQuery.point(geoPoint).distance("12km");

        QueryBuilder newQuery = templateQueryBuilder.doRewrite(queryRewriteContext);
        assertEquals(expectedQuery, newQuery);
        assertEquals(
            "{\n"
                + "  \"geo_distance\" : {\n"
                + "    \"pin.location\" : [\n"
                + "      -70.0,\n"
                + "      40.0\n"
                + "    ],\n"
                + "    \"distance\" : 12000.0,\n"
                + "    \"distance_type\" : \"arc\",\n"
                + "    \"validation_method\" : \"STRICT\",\n"
                + "    \"ignore_unmapped\" : false,\n"
                + "    \"boost\" : 1.0\n"
                + "  }\n"
                + "}",
            newQuery.toString()
        );
    }

    /**
     * Tests the doRewrite method of TemplateQueryBuilder with a range query.
     * Verifies that the template is correctly rewritten for a range query.
     */
    public void testDoRewriteWithRangeQuery() throws IOException {
        Map<String, Object> template = new HashMap<>();
        Map<String, Object> range = new HashMap<>();
        Map<String, Object> age = new HashMap<>();

        age.put("gte", "${minAge}");
        age.put("lte", "${maxAge}");
        range.put("age", age);
        template.put("range", range);

        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();
        Map<String, Object> contextVariables = new HashMap<>();
        contextVariables.put("minAge", 25);
        contextVariables.put("maxAge", 35);
        when(queryRewriteContext.getContextVariables()).thenReturn(contextVariables);

        RangeQueryBuilder expectedQuery = new RangeQueryBuilder("age");
        expectedQuery.gte(25).lte(35);

        QueryBuilder newQuery = templateQueryBuilder.doRewrite(queryRewriteContext);
        assertEquals(expectedQuery, newQuery);
        assertEquals(
            "{\n"
                + "  \"range\" : {\n"
                + "    \"age\" : {\n"
                + "      \"from\" : 25,\n"
                + "      \"to\" : 35,\n"
                + "      \"include_lower\" : true,\n"
                + "      \"include_upper\" : true,\n"
                + "      \"boost\" : 1.0\n"
                + "    }\n"
                + "  }\n"
                + "}",
            newQuery.toString()
        );
    }

    /**
     * Tests the doRewrite method of TemplateQueryBuilder with a nested map variable.
     * Verifies that the template is correctly rewritten with the nested map variable substituted.
     */
    public void testDoRewriteWithNestedMap() throws IOException {
        Map<String, Object> template = new HashMap<>();
        Map<String, Object> bool = new HashMap<>();
        List<Map<String, Object>> must = new ArrayList<>();
        Map<String, Object> match = new HashMap<>();
        Map<String, Object> textEntry = new HashMap<>();

        textEntry.put("text_entry", "${keyword}");
        match.put("match", textEntry);
        must.add(match);
        bool.put("must", must);

        List<Map<String, Object>> should = new ArrayList<>();
        Map<String, Object> shouldMatch1 = new HashMap<>();
        Map<String, Object> shouldTextEntry1 = new HashMap<>();
        shouldTextEntry1.put("text_entry", "life");
        shouldMatch1.put("match", shouldTextEntry1);
        should.add(shouldMatch1);

        Map<String, Object> shouldMatch2 = new HashMap<>();
        Map<String, Object> shouldTextEntry2 = new HashMap<>();
        shouldTextEntry2.put("text_entry", "grace");
        shouldMatch2.put("match", shouldTextEntry2);
        should.add(shouldMatch2);

        bool.put("should", should);
        bool.put("minimum_should_match", 1);

        Map<String, Object> filter = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        term.put("play_name", "Romeo and Juliet");
        filter.put("term", term);
        bool.put("filter", filter);

        template.put("bool", bool);

        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();
        Map<String, Object> contextVariables = new HashMap<>();
        contextVariables.put("keyword", "love");
        when(queryRewriteContext.getContextVariables()).thenReturn(contextVariables);

        BoolQueryBuilder expectedQuery = new BoolQueryBuilder().must(new MatchQueryBuilder("text_entry", "love"))
            .should(new MatchQueryBuilder("text_entry", "life"))
            .should(new MatchQueryBuilder("text_entry", "grace"))
            .filter(new TermQueryBuilder("play_name", "Romeo and Juliet"))
            .minimumShouldMatch(1);

        QueryBuilder newQuery = templateQueryBuilder.doRewrite(queryRewriteContext);
        assertEquals(expectedQuery, newQuery);
        assertEquals(
            "{\n"
                + "  \"bool\" : {\n"
                + "    \"must\" : [\n"
                + "      {\n"
                + "        \"match\" : {\n"
                + "          \"text_entry\" : {\n"
                + "            \"query\" : \"love\",\n"
                + "            \"operator\" : \"OR\",\n"
                + "            \"prefix_length\" : 0,\n"
                + "            \"max_expansions\" : 50,\n"
                + "            \"fuzzy_transpositions\" : true,\n"
                + "            \"lenient\" : false,\n"
                + "            \"zero_terms_query\" : \"NONE\",\n"
                + "            \"auto_generate_synonyms_phrase_query\" : true,\n"
                + "            \"boost\" : 1.0\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    ],\n"
                + "    \"filter\" : [\n"
                + "      {\n"
                + "        \"term\" : {\n"
                + "          \"play_name\" : {\n"
                + "            \"value\" : \"Romeo and Juliet\",\n"
                + "            \"boost\" : 1.0\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    ],\n"
                + "    \"should\" : [\n"
                + "      {\n"
                + "        \"match\" : {\n"
                + "          \"text_entry\" : {\n"
                + "            \"query\" : \"life\",\n"
                + "            \"operator\" : \"OR\",\n"
                + "            \"prefix_length\" : 0,\n"
                + "            \"max_expansions\" : 50,\n"
                + "            \"fuzzy_transpositions\" : true,\n"
                + "            \"lenient\" : false,\n"
                + "            \"zero_terms_query\" : \"NONE\",\n"
                + "            \"auto_generate_synonyms_phrase_query\" : true,\n"
                + "            \"boost\" : 1.0\n"
                + "          }\n"
                + "        }\n"
                + "      },\n"
                + "      {\n"
                + "        \"match\" : {\n"
                + "          \"text_entry\" : {\n"
                + "            \"query\" : \"grace\",\n"
                + "            \"operator\" : \"OR\",\n"
                + "            \"prefix_length\" : 0,\n"
                + "            \"max_expansions\" : 50,\n"
                + "            \"fuzzy_transpositions\" : true,\n"
                + "            \"lenient\" : false,\n"
                + "            \"zero_terms_query\" : \"NONE\",\n"
                + "            \"auto_generate_synonyms_phrase_query\" : true,\n"
                + "            \"boost\" : 1.0\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    ],\n"
                + "    \"adjust_pure_negative\" : true,\n"
                + "    \"minimum_should_match\" : \"1\",\n"
                + "    \"boost\" : 1.0\n"
                + "  }\n"
                + "}",
            newQuery.toString()
        );
    }

    /**
     * Tests the doRewrite method with an invalid query type.
     * Verifies that an IOException is thrown when an invalid query type is used.
     */
    public void testDoRewriteWithInvalidQueryType() throws IOException {
        Map<String, Object> template = new HashMap<>();
        template.put("invalid_query_type", new HashMap<>());
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();
        when(queryRewriteContext.getContextVariables()).thenReturn(new HashMap<>());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> templateQueryBuilder.doRewrite(queryRewriteContext)
        );
        assertTrue(exception.getMessage().contains("Failed to rewrite template query"));
    }

    /**
     * Tests the doRewrite method with a malformed JSON query.
     * Verifies that an IOException is thrown when the query JSON is malformed.
     */
    public void testDoRewriteWithMalformedJson() throws IOException {
        Map<String, Object> template = new HashMap<>();
        template.put("malformed_json", "{ this is not valid JSON }");
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();
        when(queryRewriteContext.getContextVariables()).thenReturn(new HashMap<>());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> templateQueryBuilder.doRewrite(queryRewriteContext)
        );
        assertTrue(exception.getMessage().contains("Failed to rewrite template query"));
    }

    /**
     * Tests the doRewrite method with an invalid matchall query.
     * Verifies that an IOException is thrown when an invalid matchall query is used.
     */
    public void testDoRewriteWithInvalidMatchAllQuery() throws IOException {
        Map<String, Object> template = new HashMap<>();
        template.put("matchall_1", new HashMap<>());
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();
        when(queryRewriteContext.getContextVariables()).thenReturn(new HashMap<>());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> templateQueryBuilder.doRewrite(queryRewriteContext)
        );
        assertTrue(exception.getMessage().contains("Failed to rewrite template query"));
    }

    /**
     * Tests the doRewrite method with a missing required field in a query.
     * Verifies that an IOException is thrown when a required field is missing.
     */
    public void testDoRewriteWithMissingRequiredField() throws IOException {
        Map<String, Object> template = new HashMap<>();
        template.put("term", "value");// Missing the required field for term query
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();
        when(queryRewriteContext.getContextVariables()).thenReturn(new HashMap<>());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> templateQueryBuilder.doRewrite(queryRewriteContext)
        );
        assertTrue(exception.getMessage().contains("Failed to rewrite template query"));
    }

    /**
     * Tests the doRewrite method with a malformed variable substitution.
     * Verifies that an IOException is thrown when a malformed variable is used.
     */
    public void testDoRewriteWithMalformedVariableSubstitution() throws IOException {

        Map<String, Object> template = new HashMap<>();
        Map<String, Object> terms = new HashMap<>();

        terms.put("message", "${response}");
        template.put("terms", terms);
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();

        Map<String, Object> contextVariables = new HashMap<>();
        contextVariables.put("response", "should be a list but this is a string");

        when(queryRewriteContext.getContextVariables()).thenReturn(contextVariables);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> templateQueryBuilder.doRewrite(queryRewriteContext)
        );

        assertTrue(exception.getMessage().contains("Failed to rewrite template query"));
    }

    /**
     * Tests the doRewrite method with a variable not found.
     * Verifies that an IOException is thrown when a malformed variable is used.
     */
    public void testDoRewriteWithNotFoundVariableSubstitution() throws IOException {

        Map<String, Object> template = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        message.put("value", "${response}");
        term.put("message", message);
        template.put("term", term);
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();

        Map<String, Object> contextVariables = new HashMap<>();
        contextVariables.put("response1", "foo");
        when(queryRewriteContext.getContextVariables()).thenReturn(contextVariables);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> templateQueryBuilder.doRewrite(queryRewriteContext)
        );
        assertTrue(exception.getMessage().contains("Variable not found"));
    }

    /**
     * Tests the doRewrite method of TemplateQueryBuilder with a missing bracket variable.
     * Verifies that the exception is thrown
     */
    public void testDoRewriteWithMissingBracketVariable() throws IOException {

        Map<String, Object> template = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        message.put("value", "${response");
        term.put("message", message);
        template.put("term", term);
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();

        Map<String, Object> contextVariables = new HashMap<>();
        contextVariables.put("response", "foo");
        when(queryRewriteContext.getContextVariables()).thenReturn(contextVariables);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> templateQueryBuilder.doRewrite(queryRewriteContext)
        );
        assertTrue(exception.getMessage().contains("Unclosed variable in template"));
    }

    /**
     * Tests the replaceVariables method when the template is null.
     * Verifies that an IllegalArgumentException is thrown with the appropriate error message.
     */

    public void testReplaceVariablesWithNullTemplate() {
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder((Map<String, Object>) null);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();
        Map<String, Object> contextVariables = new HashMap<>();
        contextVariables.put("response", "foo");
        when(queryRewriteContext.getContextVariables()).thenReturn(contextVariables);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> templateQueryBuilder.doRewrite(queryRewriteContext)
        );
        assertEquals("Template string cannot be null. A valid template must be provided.", exception.getMessage());
    }

    /**
     * Tests the replaceVariables method when the template is empty.
     * Verifies that an IllegalArgumentException is thrown with the appropriate error message.
     */

    public void testReplaceVariablesWithEmptyTemplate() {
        Map<String, Object> template = new HashMap<>();
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();
        Map<String, Object> contextVariables = new HashMap<>();
        contextVariables.put("response", "foo");
        when(queryRewriteContext.getContextVariables()).thenReturn(contextVariables);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> templateQueryBuilder.doRewrite(queryRewriteContext)
        );
        assertEquals("Template string cannot be empty. A valid template must be provided.", exception.getMessage());

    }

    /**
     * Tests the replaceVariables method when the variables map is null.
     * Verifies that the method returns the original template unchanged,
     * since a null variables map is treated as no replacement.
     */
    public void testReplaceVariablesWithNullVariables() throws IOException {

        Map<String, Object> template = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        Map<String, Object> message = new HashMap<>();

        message.put("value", "foo");
        term.put("message", message);
        template.put("term", term);
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(template);
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("message", "foo");

        QueryCoordinatorContext queryRewriteContext = mockQueryRewriteContext();

        when(queryRewriteContext.getContextVariables()).thenReturn(null);

        TermQueryBuilder newQuery = (TermQueryBuilder) templateQueryBuilder.doRewrite(queryRewriteContext);

        assertEquals(newQuery, termQueryBuilder);
        assertEquals(
            "{\n"
                + "  \"term\" : {\n"
                + "    \"message\" : {\n"
                + "      \"value\" : \"foo\",\n"
                + "      \"boost\" : 1.0\n"
                + "    }\n"
                + "  }\n"
                + "}",
            newQuery.toString()
        );
    }

    /**
     * Helper method to create a mock QueryCoordinatorContext for testing.
     */
    private QueryCoordinatorContext mockQueryRewriteContext() {
        QueryCoordinatorContext queryRewriteContext = mock(QueryCoordinatorContext.class);
        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            BiConsumer<Client, ActionListener<?>> biConsumer = invocation.getArgument(0);
            biConsumer.accept(
                null,
                ActionListener.wrap(
                    response -> inProgressLatch.countDown(),
                    err -> fail("Failed to set query tokens supplier: " + err.getMessage())
                )
            );
            return null;
        }).when(queryRewriteContext).registerAsyncAction(any());

        NamedXContentRegistry TEST_XCONTENT_REGISTRY_FOR_QUERY = new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
        );
        when(queryRewriteContext.getXContentRegistry()).thenReturn(TEST_XCONTENT_REGISTRY_FOR_QUERY);

        return queryRewriteContext;
    }
}
