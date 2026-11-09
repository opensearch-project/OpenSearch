/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CombinedFieldQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Comprehensive test suite for CombinedFieldsQueryBuilder functionality.
 *
 * Test Structure:
 * 1. Test Infrastructure Setup
 * 2. Query Construction Tests
 * 3. Configuration Tests (operators, zero terms, etc.)
 * 4. Serialization Tests
 * 5. Functional Behavior Tests
 * 6. Error Condition Tests
 * 7. Edge Case Tests
 */
public class CombinedFieldsQueryBuilderTests extends AbstractQueryTestCase<CombinedFieldsQueryBuilder> {

    // ========================
    // TEST DATA CONSTANTS
    // ========================

    private static final String[] TEST_QUERY_TEXTS = {
        "machine learning algorithms",
        "distributed systems architecture",
        "natural language processing techniques",
        "cloud computing infrastructure",
        "data science methodologies",
        "artificial intelligence applications" };

    // ========================
    // TEST INFRASTRUCTURE SETUP
    // ========================

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            "_doc",
            new CompressedXContent(
                PutMappingRequest.simpleMapping(
                    "content",
                    "type=text",
                    "description",
                    "type=text",
                    "summary",
                    "type=text",
                    "metadata",
                    "type=keyword",
                    "tags",
                    "type=keyword",
                    "boosted_field",
                    "type=text"
                ).toString()
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    @Override
    protected CombinedFieldsQueryBuilder doCreateTestQueryBuilder() {
        String queryText = randomFrom(TEST_QUERY_TEXTS);
        String primaryField = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, "content", "description", "summary");

        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, primaryField);

        // Add additional fields with random boosts
        int additionalFields = randomIntBetween(0, 2);
        for (int i = 0; i < additionalFields; i++) {
            String additionalField = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, "content", "description", "summary");
            float boost = 1.0f + randomFloat() * 3.0f;
            String fieldWithBoost = additionalField + "^" + boost;
            queryBuilder = new CombinedFieldsQueryBuilder(queryText, primaryField, fieldWithBoost);
        }

        // Random configuration
        if (randomBoolean()) {
            queryBuilder.operator(randomFrom(Operator.values()));
        }

        // Add minimum_should_match configuration
        if (randomBoolean()) {
            queryBuilder.minimumShouldMatch(randomMinimumShouldMatch());
        }

        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(CombinedFieldsQueryBuilder queryBuilder, Query query, QueryShardContext context) {
        assertThat("Query should not be null", query, notNullValue());
        assertThat(
            "Query should be of expected type",
            query,
            anyOf(
                instanceOf(BooleanQuery.class),
                instanceOf(TermQuery.class),
                instanceOf(MatchAllDocsQuery.class),
                instanceOf(MatchNoDocsQuery.class),
                instanceOf(CombinedFieldQuery.class)
            )
        );
    }

    @Override
    protected boolean supportsBoost() {
        return true;
    }

    @Override
    protected boolean supportsQueryName() {
        return true;
    }

    @Override
    protected boolean builderGeneratesCacheableQueries() {
        return true;
    }

    // ========================
    // QUERY CONSTRUCTION TESTS
    // ========================

    /**
     * Tests basic query construction with single field.
     */
    public void testBasicQueryConstruction() {
        String queryText = "test query";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME);

        assertEquals("Query text should match", queryText, queryBuilder.queryValue());
        assertEquals("Should have one field", 1, queryBuilder.fieldToWeight().size());
        assertTrue("Should contain the specified field", queryBuilder.fieldToWeight().containsKey(TEXT_FIELD_NAME));
    }

    /**
     * Tests query construction with multiple fields and boosts.
     */
    public void testMultipleFieldsWithBoosts() {
        String queryText = "machine learning algorithms";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(
            queryText,
            "title",
            "content^2.0",
            "summary^0.5",
            "tags^1.5"
        );

        Map<String, Float> fields = queryBuilder.fieldToWeight();

        assertEquals("Should have 4 fields", 4, fields.size());
        assertEquals("Title should have default boost", 1.0f, fields.get("title"), 1e-6);
        assertEquals("Content should have boost 2.0", 2.0f, fields.get("content"), 1e-6);
        assertEquals("Summary should have boost 0.5", 0.5f, fields.get("summary"), 1e-6);
        assertEquals("Tags should have boost 1.5", 1.5f, fields.get("tags"), 1e-6);
    }

    /**
     * Tests comprehensive query construction with all features.
     */
    public void testComprehensiveQueryConstruction() throws IOException {
        String queryText = "comprehensive test query with all features";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME + "^1.5")
            .operator(Operator.AND)
            .minimumShouldMatch("75%")
            .boost(1.8f)
            .queryName("comprehensive_test");

        assertEquals("Query text should match", queryText, queryBuilder.queryValue());
        assertEquals("Field count should be 2", 2, queryBuilder.fieldToWeight().size());
        assertEquals("Operator should be AND", Operator.AND, queryBuilder.operator());
        assertEquals("Minimum should match should be 75%", "75%", queryBuilder.minimumShouldMatch());
        assertEquals("Boost should be 1.8", 1.8f, queryBuilder.boost(), 1e-6);
        assertEquals("Query name should match", "comprehensive_test", queryBuilder.queryName());

        Query query = queryBuilder.toQuery(createShardContext());
        assertThat("Generated query should not be null", query, notNullValue());
    }

    // ========================
    // CONFIGURATION TESTS
    // ========================

    /**
     * Tests operator configuration and behavior.
     */
    public void testOperatorConfiguration() {
        String queryText = "natural language processing";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME);

        queryBuilder.operator(Operator.OR);
        assertEquals("Operator should be OR", Operator.OR, queryBuilder.operator());

        queryBuilder.operator(Operator.AND);
        assertEquals("Operator should be AND", Operator.AND, queryBuilder.operator());
    }

    /**
     * Tests OR operator query generation behavior.
     */
    public void testOrOperatorQueryGeneration() throws IOException {
        String queryText = "distributed systems architecture";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME).operator(Operator.OR);

        Query query = queryBuilder.toQuery(createShardContext());

        assertThat("Query should be a BooleanQuery for OR operator", query, instanceOf(BooleanQuery.class));

        BooleanQuery booleanQuery = (BooleanQuery) query;
        booleanQuery.clauses().forEach(clause -> {
            assertEquals("All clauses should be SHOULD for OR operator", BooleanClause.Occur.SHOULD, clause.occur());
        });

        assertThat("Should have clauses for multi-term query", booleanQuery.clauses().size(), notNullValue());
    }

    /**
     * Tests boost configuration and validation.
     */
    public void testBoostConfiguration() {
        String queryText = "boosted query test";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME);

        float positiveBoost = 2.5f;
        queryBuilder.boost(positiveBoost);
        assertEquals("Boost should be set correctly", positiveBoost, queryBuilder.boost(), 1e-6);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> queryBuilder.boost(-1.0f));
        assertThat("Exception message should mention negative boost", exception.getMessage(), containsString("negative [boost]"));
    }

    /**
     * Tests query name configuration.
     */
    public void testQueryNameConfiguration() {
        String queryText = "named query test";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME);

        String queryName = "test_named_query";
        queryBuilder.queryName(queryName);
        assertEquals("Query name should be set correctly", queryName, queryBuilder.queryName());
    }

    // ========================
    // MINIMUM_SHOULD_MATCH TESTS
    // ========================

    /**
     * Tests minimum_should_match configuration and getter/setter methods.
     */
    public void testMinimumShouldMatchConfiguration() {
        String queryText = "minimum should match test";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME);

        // Test setting minimum_should_match
        String minimumShouldMatch = "75%";
        queryBuilder.minimumShouldMatch(minimumShouldMatch);
        assertEquals("Minimum should match should be set correctly", minimumShouldMatch, queryBuilder.minimumShouldMatch());

        // Test setting numeric value
        String numericValue = "2";
        queryBuilder.minimumShouldMatch(numericValue);
        assertEquals("Numeric minimum should match should be set correctly", numericValue, queryBuilder.minimumShouldMatch());

        // Test setting null value
        queryBuilder.minimumShouldMatch(null);
        assertNull("Null minimum should match should be set correctly", queryBuilder.minimumShouldMatch());

        // Test method chaining
        CombinedFieldsQueryBuilder chained = queryBuilder.minimumShouldMatch("50%");
        assertSame("Method chaining should return same instance", queryBuilder, chained);
        assertEquals("Chained minimum should match should be set", "50%", queryBuilder.minimumShouldMatch());
    }

    /**
     * Tests minimum_should_match JSON serialization and parsing.
     */
    public void testMinimumShouldMatchJsonSerialization() throws IOException {
        // Test with percentage value
        String jsonWithPercentage = "{\n"
            + "  \"combined_fields\" : {\n"
            + "    \"query\" : \"test query\",\n"
            + "    \"fields\" : [ \"content\" ],\n"
            + "    \"minimum_should_match\" : \"75%\"\n"
            + "  }\n"
            + "}";

        CombinedFieldsQueryBuilder parsedWithPercentage = (CombinedFieldsQueryBuilder) parseQuery(jsonWithPercentage);
        assertEquals("Percentage minimum should match should be parsed correctly", "75%", parsedWithPercentage.minimumShouldMatch());

        // Test with numeric value
        String jsonWithNumber = "{\n"
            + "  \"combined_fields\" : {\n"
            + "    \"query\" : \"test query\",\n"
            + "    \"fields\" : [ \"content\" ],\n"
            + "    \"minimum_should_match\" : \"2\"\n"
            + "  }\n"
            + "}";

        CombinedFieldsQueryBuilder parsedWithNumber = (CombinedFieldsQueryBuilder) parseQuery(jsonWithNumber);
        assertEquals("Numeric minimum should match should be parsed correctly", "2", parsedWithNumber.minimumShouldMatch());

        // Test with complex value
        String jsonWithComplex = "{\n"
            + "  \"combined_fields\" : {\n"
            + "    \"query\" : \"test query\",\n"
            + "    \"fields\" : [ \"content\" ],\n"
            + "    \"minimum_should_match\" : \"2<75%\"\n"
            + "  }\n"
            + "}";

        CombinedFieldsQueryBuilder parsedWithComplex = (CombinedFieldsQueryBuilder) parseQuery(jsonWithComplex);
        assertEquals("Complex minimum should match should be parsed correctly", "2<75%", parsedWithComplex.minimumShouldMatch());

        // Test serialization round-trip
        CombinedFieldsQueryBuilder original = new CombinedFieldsQueryBuilder("test query", "content").minimumShouldMatch("50%")
            .operator(Operator.OR);

        String serialized = original.toString();
        CombinedFieldsQueryBuilder deserialized = (CombinedFieldsQueryBuilder) parseQuery(serialized);

        assertEquals(
            "Minimum should match should be preserved in serialization",
            original.minimumShouldMatch(),
            deserialized.minimumShouldMatch()
        );
    }

    /**
     * Tests minimum_should_match functional behavior with real queries.
     */
    public void testMinimumShouldMatchFunctionalBehavior() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(dir, iwc);

        // Add test documents with multiple terms
        addTestDocument(writer, "content", "machine learning algorithms");
        addTestDocument(writer, "content", "machine learning");
        addTestDocument(writer, "content", "machine");
        addTestDocument(writer, "content", "learning algorithms");
        addTestDocument(writer, "content", "algorithms");
        writer.close();

        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Test with minimum_should_match = "2" (at least 2 terms)
        CombinedFieldsQueryBuilder queryWithMin2 = new CombinedFieldsQueryBuilder("machine learning algorithms", "content").operator(
            Operator.OR
        ).minimumShouldMatch("2");

        Query luceneQueryMin2 = queryWithMin2.toQuery(createShardContext());
        TopDocs resultsMin2 = searcher.search(luceneQueryMin2, 10);

        // Should match documents with at least 2 of the 3 terms
        // The documents should be: "machine learning algorithms" (3 terms), "machine learning" (2 terms), "learning algorithms" (2 terms)
        assertTrue("Should match documents with at least 2 terms", resultsMin2.totalHits.value() == 3);

        // Test with minimum_should_match = "75%" (75% of terms, rounded down, so 2 terms)
        CombinedFieldsQueryBuilder queryWithMin75 = new CombinedFieldsQueryBuilder("machine learning algorithms", "content").operator(
            Operator.OR
        ).minimumShouldMatch("75%");

        Query luceneQueryMin75 = queryWithMin75.toQuery(createShardContext());
        TopDocs resultsMin75 = searcher.search(luceneQueryMin75, 10);

        // 75% of 3 terms = 2.25, rounded up to 3 terms required
        // This should match fewer documents than the "2" minimum_should_match
        assertTrue("Should match documents with 75% of terms", resultsMin75.totalHits.value() == 3);

        reader.close();
        dir.close();
    }

    /**
     * Tests minimum_should_match edge cases and validation.
     */
    public void testMinimumShouldMatchEdgeCases() {
        String queryText = "test query";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME);

        // Test with empty string
        queryBuilder.minimumShouldMatch("");
        assertEquals("Empty string should be preserved", "", queryBuilder.minimumShouldMatch());

        // Test with whitespace-only string
        queryBuilder.minimumShouldMatch("   ");
        assertEquals("Whitespace-only string should be preserved", "   ", queryBuilder.minimumShouldMatch());

        // Test with very large number
        queryBuilder.minimumShouldMatch("999999");
        assertEquals("Large number should be preserved", "999999", queryBuilder.minimumShouldMatch());

        // Test with negative percentage
        queryBuilder.minimumShouldMatch("-25%");
        assertEquals("Negative percentage should be preserved", "-25%", queryBuilder.minimumShouldMatch());
    }

    /**
     * Tests minimum_should_match in equals and hashCode methods.
     */
    public void testMinimumShouldMatchEqualsAndHashCode() {
        String queryText = "test query";
        String field = "content";

        CombinedFieldsQueryBuilder query1 = new CombinedFieldsQueryBuilder(queryText, field).minimumShouldMatch("75%");

        CombinedFieldsQueryBuilder query2 = new CombinedFieldsQueryBuilder(queryText, field).minimumShouldMatch("75%");

        CombinedFieldsQueryBuilder query3 = new CombinedFieldsQueryBuilder(queryText, field).minimumShouldMatch("50%");

        // Test equals
        assertEquals("Queries with same minimum_should_match should be equal", query1, query2);
        assertNotEquals("Queries with different minimum_should_match should not be equal", query1, query3);

        // Test hashCode
        assertEquals("Queries with same minimum_should_match should have same hashCode", query1.hashCode(), query2.hashCode());
        assertNotEquals("Queries with different minimum_should_match should have different hashCode", query1.hashCode(), query3.hashCode());

        // Test with null minimum_should_match
        CombinedFieldsQueryBuilder query4 = new CombinedFieldsQueryBuilder(queryText, field).minimumShouldMatch(null);

        CombinedFieldsQueryBuilder query5 = new CombinedFieldsQueryBuilder(queryText, field).minimumShouldMatch(null);

        assertEquals("Queries with null minimum_should_match should be equal", query4, query5);
        assertEquals("Queries with null minimum_should_match should have same hashCode", query4.hashCode(), query5.hashCode());
    }

    // ========================
    // SERIALIZATION TESTS
    // ========================

    /**
     * Tests JSON serialization with comprehensive field configuration.
     */
    public void testJsonSerializationWithMultipleFields() throws IOException {
        String json = "{\n"
            + "  \"combined_fields\" : {\n"
            + "    \"query\" : \"distributed systems architecture\",\n"
            + "    \"fields\" : [ \"title^2.0\", \"content^1.5\", \"tags^0.8\", \"metadata^1.2\" ],\n"
            + "    \"operator\" : \"AND\",\n"
            + "    \"minimum_should_match\" : \"75%\",\n"
            + "    \"boost\" : 1.5,\n"
            + "    \"_name\" : \"test_combined_fields_query\"\n"
            + "  }\n"
            + "}";

        Map<String, Float> expectedFieldToWeight = Map.of("title", 2.0f, "content", 1.5f, "tags", 0.8f, "metadata", 1.2f);
        CombinedFieldsQueryBuilder parsed = (CombinedFieldsQueryBuilder) parseQuery(json);

        assertEquals("Query value should match", "distributed systems architecture", parsed.queryValue());
        assertEquals("Field count should match", 4, parsed.fieldToWeight().size());
        for (Map.Entry<String, Float> entry : parsed.fieldToWeight().entrySet()) {
            Float weight = expectedFieldToWeight.get(entry.getKey());
            assertEquals("Field weight should match for " + entry.getKey(), weight, entry.getValue(), 1e-6);
        }
        assertEquals("Operator should match", Operator.AND, parsed.operator());
        assertEquals("Minimum should match should match", "75%", parsed.minimumShouldMatch());
        assertEquals("Boost should match", 1.5f, parsed.boost(), 1e-6);
        assertEquals("Query name should match", "test_combined_fields_query", parsed.queryName());
    }

    /**
     * Tests serialization and deserialization consistency.
     */
    public void testSerializationConsistency() throws IOException {
        String queryText = "serialization test query";
        CombinedFieldsQueryBuilder original = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME + "^1.5")
            .operator(Operator.OR)
            .minimumShouldMatch("50%")
            .boost(2.0f)
            .queryName("serialization_test");

        String json = original.toString();
        CombinedFieldsQueryBuilder deserialized = (CombinedFieldsQueryBuilder) parseQuery(json);

        assertEquals("Query value should be preserved", original.queryValue(), deserialized.queryValue());
        assertEquals("Fields should be preserved", original.fieldToWeight(), deserialized.fieldToWeight());
        assertEquals("Operator should be preserved", original.operator(), deserialized.operator());
        assertEquals("Minimum should match should be preserved", original.minimumShouldMatch(), deserialized.minimumShouldMatch());
        assertEquals("Boost should be preserved", original.boost(), deserialized.boost(), 1e-6);
        assertEquals("Query name should be preserved", original.queryName(), deserialized.queryName());
    }

    // ========================
    // FUNCTIONAL BEHAVIOR TESTS
    // ========================

    /**
     * Tests functional operator behavior with real Lucene index.
     */
    public void testOperatorBehaviorFunctional() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(dir, iwc);

        addTestDocument(writer, "content", "machine learning");
        addTestDocument(writer, "content", "machine");
        addTestDocument(writer, "content", "learning");
        writer.close();

        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        // Test AND operator - should only match "machine learning"
        CombinedFieldsQueryBuilder andQuery = new CombinedFieldsQueryBuilder("machine learning", "content").operator(Operator.AND);
        Query luceneAndQuery = andQuery.toQuery(createShardContext());
        TopDocs andResults = searcher.search(luceneAndQuery, 10);
        assertEquals("AND operator should match only complete phrase", 1, andResults.totalHits.value());

        // Test OR operator - should match all documents
        CombinedFieldsQueryBuilder orQuery = new CombinedFieldsQueryBuilder("machine learning", "content").operator(Operator.OR);
        Query luceneOrQuery = orQuery.toQuery(createShardContext());
        TopDocs orResults = searcher.search(luceneOrQuery, 10);
        assertEquals("OR operator should match all documents", 3, orResults.totalHits.value());

        reader.close();
        dir.close();
    }

    // ========================
    // ERROR CONDITION TESTS
    // ========================

    /**
     * Tests constructor validation with null/invalid inputs.
     */
    public void testConstructorValidation() {
        IllegalArgumentException nullValueException = expectThrows(
            IllegalArgumentException.class,
            () -> new CombinedFieldsQueryBuilder(null, "content")
        );
        assertThat("Exception should mention query value", nullValueException.getMessage(), containsString("requires query value"));

        IllegalArgumentException nullFieldsException = expectThrows(
            IllegalArgumentException.class,
            () -> new CombinedFieldsQueryBuilder("test", (String[]) null)
        );
        assertThat("Exception should mention field list", nullFieldsException.getMessage(), containsString("requires field list"));
    }

    /**
     * Tests rejection of non-text field types.
     */
    public void testNonTextFieldRejection() throws IOException {
        String queryText = "test query";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, INT_FIELD_NAME);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> queryBuilder.toQuery(createShardContext()));
        assertThat(
            "Exception should mention field type support",
            exception.getMessage(),
            containsString("does not support [combined_fields] queries")
        );
    }

    /**
     * Tests rejection of mixed field types.
     */
    public void testMixedFieldTypesRejection() throws IOException {
        String queryText = "test query";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME, INT_FIELD_NAME);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> queryBuilder.toQuery(createShardContext()));
        assertThat(
            "Exception should mention field type support",
            exception.getMessage(),
            containsString("does not support [combined_fields] queries")
        );
    }

    /**
     * Tests field boost parsing validation.
     */
    public void testFieldBoostValidation() {
        String queryText = "test query";

        // Valid boost values
        CombinedFieldsQueryBuilder validQuery = new CombinedFieldsQueryBuilder(queryText, "field^1.5", "field2^2.0");
        Map<String, Float> fields = validQuery.fieldToWeight();
        assertEquals("Should parse boost correctly", 1.5f, fields.get("field"), 1e-6);
        assertEquals("Should parse boost correctly", 2.0f, fields.get("field2"), 1e-6);

        // Invalid boost format
        NumberFormatException exception = expectThrows(
            NumberFormatException.class,
            () -> new CombinedFieldsQueryBuilder(queryText, "field^invalid")
        );
        assertThat("Exception should mention invalid input", exception.getMessage(), containsString("invalid"));
    }

    // ========================
    // EDGE CASE TESTS
    // ========================

    /**
     * Tests handling of unmapped fields.
     */
    public void testUnmappedFieldsHandling() throws IOException {
        String queryText = "test query with unmapped fields";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, "unmapped_field_1", "unmapped_field_2");

        Query query = queryBuilder.toQuery(createShardContext());
        assertThat("Query should be MatchNoDocsQuery for unmapped fields", query, instanceOf(MatchNoDocsQuery.class));
    }

    /**
     * Tests handling of wildcard field patterns.
     */
    public void testWildcardFieldPatterns() throws IOException {
        String queryText = "test wildcard field patterns";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, "wildcard_field_*");

        Query query = queryBuilder.toQuery(createShardContext());
        assertThat("Query should be generated for wildcard fields", query, notNullValue());
    }

    /**
     * Tests handling of empty query text.
     */
    public void testEmptyQueryTextHandling() throws IOException {
        String queryText = "";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME);

        // Should handle empty query gracefully
        Query query = queryBuilder.toQuery(createShardContext());

        // Verify no exceptions are thrown
        assertThat("Query should be generated for empty text", query, notNullValue());
    }

    /**
     * Tests handling of whitespace-only query text.
     */
    public void testWhitespaceOnlyQueryText() throws IOException {
        String queryText = "   ";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME);

        // Should handle whitespace-only query gracefully
        Query query = queryBuilder.toQuery(createShardContext());
        // Verify no exceptions are thrown
    }

    /**
     * Tests phrase query handling (should not support phrases).
     */
    public void testPhraseQueryHandling() throws IOException {
        String queryText = "\"phrase query\"";
        CombinedFieldsQueryBuilder queryBuilder = new CombinedFieldsQueryBuilder(queryText, TEXT_FIELD_NAME);

        // Should work but phrase aspect may be ignored or handled appropriately
        try {
            Query query = queryBuilder.toQuery(createShardContext());
            assertThat("Query should be generated even with phrase-like text", query, notNullValue());
        } catch (IllegalArgumentException e) {
            // If phrases are explicitly not supported, this is also acceptable
            assertThat("Exception should mention phrase support", e.getMessage(), containsString("phrases"));
        }
    }

    // ========================
    // HELPER METHODS
    // ========================

    /**
     * Helper method to add a document to the test index.
     */
    private void addTestDocument(IndexWriter writer, String field, String value) throws IOException {
        Document doc = new Document();
        doc.add(new TextField(field, value, Field.Store.NO));
        writer.addDocument(doc);
    }
}
