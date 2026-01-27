/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.Operator;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.SimpleQueryStringFlag;
import org.opensearch.protobufs.MinimumShouldMatch;
import org.opensearch.protobufs.SimpleQueryStringFlags;
import org.opensearch.protobufs.SimpleQueryStringQuery;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class SimpleQueryStringBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithOnlyQuery() {
        // Create a protobuf SimpleQueryStringQuery with only required field
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder().setQuery("test query").build();

        // Call the method under test
        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);

        // Verify the result
        assertNotNull("SimpleQueryStringBuilder should not be null", result);
        assertEquals("Query text should match", "test query", result.value());
        assertEquals("Boost should be default", 1.0f, result.boost(), 0.0f);
        assertNull("Query name should be null", result.queryName());
        assertNull("Analyzer should be null", result.analyzer());
        assertEquals("Default operator should be OR", Operator.OR, result.defaultOperator());
        // Note: flags() is package-private, so we can't directly test it here
        // The default value is ALL which gets set correctly by the implementation
        assertFalse("Lenient should be false", result.lenient());
        assertFalse("Analyze wildcard should be false", result.analyzeWildcard());
        assertNull("Minimum should match should be null", result.minimumShouldMatch());
        assertNull("Quote field suffix should be null", result.quoteFieldSuffix());
        assertTrue("Auto generate synonyms phrase query should be true", result.autoGenerateSynonymsPhraseQuery());
        assertEquals(
            "Fuzzy prefix length should be default",
            SimpleQueryStringBuilder.DEFAULT_FUZZY_PREFIX_LENGTH,
            result.fuzzyPrefixLength()
        );
        assertEquals(
            "Fuzzy max expansions should be default",
            SimpleQueryStringBuilder.DEFAULT_FUZZY_MAX_EXPANSIONS,
            result.fuzzyMaxExpansions()
        );
        assertTrue("Fuzzy transpositions should be true", result.fuzzyTranspositions());
    }

    public void testFromProtoWithAllFields() {
        // Create a protobuf SimpleQueryStringQuery with all fields
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("search text")
            .addFields("title^2.0")
            .addFields("content")
            .addFields("author^1.5")
            .setBoost(2.5f)
            .setAnalyzer("standard")
            .setDefaultOperator(org.opensearch.protobufs.Operator.OPERATOR_AND)
            .setFlags(
                SimpleQueryStringFlags.newBuilder()
                    .setSingle(org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_AND)
                    .build()
            )
            .setLenient(true)
            .setAnalyzeWildcard(true)
            .setXName("my_query")
            .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().setString("75%").build())
            .setQuoteFieldSuffix(".exact")
            .setAutoGenerateSynonymsPhraseQuery(false)
            .setFuzzyPrefixLength(2)
            .setFuzzyMaxExpansions(100)
            .setFuzzyTranspositions(false)
            .build();

        // Call the method under test
        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);

        // Verify the result
        assertNotNull("SimpleQueryStringBuilder should not be null", result);
        assertEquals("Query text should match", "search text", result.value());
        assertEquals("Boost should match", 2.5f, result.boost(), 0.0f);
        assertEquals("Query name should match", "my_query", result.queryName());
        assertEquals("Analyzer should match", "standard", result.analyzer());
        assertEquals("Default operator should be AND", Operator.AND, result.defaultOperator());
        // Note: flags() is package-private, verified by successful creation
        assertTrue("Lenient should be true", result.lenient());
        assertTrue("Analyze wildcard should be true", result.analyzeWildcard());
        assertEquals("Minimum should match should be 75%", "75%", result.minimumShouldMatch());
        assertEquals("Quote field suffix should match", ".exact", result.quoteFieldSuffix());
        assertFalse("Auto generate synonyms phrase query should be false", result.autoGenerateSynonymsPhraseQuery());
        assertEquals("Fuzzy prefix length should match", 2, result.fuzzyPrefixLength());
        assertEquals("Fuzzy max expansions should match", 100, result.fuzzyMaxExpansions());
        assertFalse("Fuzzy transpositions should be false", result.fuzzyTranspositions());

        // Verify fields
        Map<String, Float> fields = result.fields();
        assertEquals("Should have 3 fields", 3, fields.size());
        assertTrue("Should contain title field", fields.containsKey("title"));
        assertEquals("title boost should be 2.0", 2.0f, fields.get("title"), 0.0f);
        assertTrue("Should contain content field", fields.containsKey("content"));
        assertEquals("content boost should be 1.0", 1.0f, fields.get("content"), 0.0f);
        assertTrue("Should contain author field", fields.containsKey("author"));
        assertEquals("author boost should be 1.5", 1.5f, fields.get("author"), 0.0f);
    }

    public void testFromProtoWithMissingQuery() {
        // Create a protobuf SimpleQueryStringQuery without query text
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder().build();

        // Call the method under test and expect exception
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SimpleQueryStringBuilderProtoUtils.fromProto(query)
        );

        assertEquals(
            "Exception message should match constant",
            SimpleQueryStringBuilder.QUERY_TEXT_MISSING,
            exception.getMessage()
        );
    }

    public void testFromProtoWithEmptyQuery() {
        // Create a protobuf SimpleQueryStringQuery with empty query text
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder().setQuery("").build();

        // Call the method under test and expect exception
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SimpleQueryStringBuilderProtoUtils.fromProto(query)
        );

        assertEquals(
            "Exception message should match constant",
            SimpleQueryStringBuilder.QUERY_TEXT_MISSING,
            exception.getMessage()
        );
    }

    public void testFromProtoWithFieldsNoBoost() {
        // Create a protobuf SimpleQueryStringQuery with fields without boost notation
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .addFields("title")
            .addFields("content")
            .build();

        // Call the method under test
        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);

        // Verify fields
        Map<String, Float> fields = result.fields();
        assertEquals("Should have 2 fields", 2, fields.size());
        assertTrue("Should contain title field", fields.containsKey("title"));
        assertEquals("title boost should be 1.0", 1.0f, fields.get("title"), 0.0f);
        assertTrue("Should contain content field", fields.containsKey("content"));
        assertEquals("content boost should be 1.0", 1.0f, fields.get("content"), 0.0f);
    }

    public void testFromProtoWithInvalidBoostNotation() {
        // Create a protobuf SimpleQueryStringQuery with invalid boost notation
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .addFields("title^invalid")
            .build();

        // Call the method under test - should default to 1.0
        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);

        // Verify field is treated as whole string with default boost
        Map<String, Float> fields = result.fields();
        assertEquals("Should have 1 field", 1, fields.size());
        assertTrue("Should contain the whole field string", fields.containsKey("title^invalid"));
        assertEquals("Boost should be default", 1.0f, fields.get("title^invalid"), 0.0f);
    }

    public void testFromProtoWithFlagsSingle() {
        // Test all single flag values - we can't directly test the flags() method
        // because it's package-private, but we can verify the query builds successfully
        org.opensearch.protobufs.SimpleQueryStringFlag[] protoFlags = {
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_ALL,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_NONE,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_AND,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_OR,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_NOT,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_PREFIX,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_PHRASE,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_PRECEDENCE,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_ESCAPE,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_WHITESPACE,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_FUZZY,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_NEAR,
            org.opensearch.protobufs.SimpleQueryStringFlag.SIMPLE_QUERY_STRING_FLAG_SLOP
        };

        for (org.opensearch.protobufs.SimpleQueryStringFlag protoFlag : protoFlags) {
            SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
                .setQuery("test")
                .setFlags(SimpleQueryStringFlags.newBuilder().setSingle(protoFlag).build())
                .build();

            SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);
            // Note: flags() is package-private, so we just verify the query builds successfully
            assertNotNull("Query should build successfully for flag " + protoFlag, result);
            assertEquals("Query text should match", "test", result.value());
        }
    }

    public void testFromProtoWithFlagsMultiple() {
        // Test multiple flags as pipe-delimited string
        // Note: flags() is package-private, so we just verify the query builds successfully
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .setFlags(SimpleQueryStringFlags.newBuilder().setMultiple("AND|OR|NOT").build())
            .build();

        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);
        assertNotNull("Query should build successfully", result);
        assertEquals("Query text should match", "test", result.value());
    }

    public void testFromProtoWithFlagsMultipleAll() {
        // Test multiple flags with ALL
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .setFlags(SimpleQueryStringFlags.newBuilder().setMultiple("ALL").build())
            .build();

        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);
        assertNotNull("Query should build successfully", result);
        assertEquals("Query text should match", "test", result.value());
    }

    public void testFromProtoWithFlagsMultipleNone() {
        // Test multiple flags with NONE
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .setFlags(SimpleQueryStringFlags.newBuilder().setMultiple("NONE").build())
            .build();

        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);
        assertNotNull("Query should build successfully", result);
        assertEquals("Query text should match", "test", result.value());
    }

    public void testFromProtoWithFlagsMultipleInvalid() {
        // Test multiple flags with invalid flag
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .setFlags(SimpleQueryStringFlags.newBuilder().setMultiple("INVALID_FLAG").build())
            .build();

        // Call the method under test and expect exception
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SimpleQueryStringBuilderProtoUtils.fromProto(query)
        );

        assertTrue("Exception message should mention unknown flag", exception.getMessage().contains("Unknown"));
        assertTrue("Exception message should mention the flag name", exception.getMessage().contains("INVALID_FLAG"));
    }

    public void testFromProtoWithMinimumShouldMatchInt() {
        // Test minimum_should_match as int32
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().setInt32(2).build())
            .build();

        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);
        assertEquals("Minimum should match should be '2'", "2", result.minimumShouldMatch());
    }

    public void testFromProtoWithMinimumShouldMatchString() {
        // Test minimum_should_match as string
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .setMinimumShouldMatch(MinimumShouldMatch.newBuilder().setString("75%").build())
            .build();

        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);
        assertEquals("Minimum should match should be '75%'", "75%", result.minimumShouldMatch());
    }

    public void testFromProtoWithOperatorOr() {
        // Test default_operator as OR
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .setDefaultOperator(org.opensearch.protobufs.Operator.OPERATOR_OR)
            .build();

        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);
        assertEquals("Default operator should be OR", Operator.OR, result.defaultOperator());
    }

    public void testFromProtoWithOperatorAnd() {
        // Test default_operator as AND
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .setDefaultOperator(org.opensearch.protobufs.Operator.OPERATOR_AND)
            .build();

        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);
        assertEquals("Default operator should be AND", Operator.AND, result.defaultOperator());
    }

    public void testFromProtoWithEmptyFlagsString() {
        // Test with empty flags string - should default to ALL
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .setFlags(SimpleQueryStringFlags.newBuilder().setMultiple("").build())
            .build();

        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);
        assertNotNull("Query should build successfully", result);
        assertEquals("Query text should match", "test", result.value());
    }

    public void testFromProtoWithMultipleFlagsWithSpaces() {
        // Test multiple flags with spaces around pipe
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .setFlags(SimpleQueryStringFlags.newBuilder().setMultiple("AND | OR | NOT").build())
            .build();

        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);
        assertNotNull("Query should build successfully", result);
        assertEquals("Query text should match", "test", result.value());
    }

    public void testFromProtoWithEmptyFieldsList() {
        // Test with no fields specified
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder().setQuery("test").build();

        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);

        // Verify fields map is empty (will use default fields from index settings)
        Map<String, Float> fields = result.fields();
        assertEquals("Fields should be empty", 0, fields.size());
    }

    public void testFromProtoWithFuzzySettings() {
        // Test fuzzy-related settings
        SimpleQueryStringQuery query = SimpleQueryStringQuery.newBuilder()
            .setQuery("test")
            .setFuzzyPrefixLength(3)
            .setFuzzyMaxExpansions(150)
            .setFuzzyTranspositions(true)
            .build();

        SimpleQueryStringBuilder result = SimpleQueryStringBuilderProtoUtils.fromProto(query);
        assertEquals("Fuzzy prefix length should match", 3, result.fuzzyPrefixLength());
        assertEquals("Fuzzy max expansions should match", 150, result.fuzzyMaxExpansions());
        assertTrue("Fuzzy transpositions should be true", result.fuzzyTranspositions());
    }
}
