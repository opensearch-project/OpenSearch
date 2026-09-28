/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.rewriters;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MustNotToShouldRewriterTests extends OpenSearchTestCase {

    private final MustNotToShouldRewriter rewriter = MustNotToShouldRewriter.INSTANCE;
    private QueryShardContext context;
    private Directory directory;
    private IndexReader reader;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        context = mock(QueryShardContext.class);

        // Create an index with single-valued numeric fields
        directory = newDirectory();
        IndexWriterConfig config = new IndexWriterConfig(Lucene.STANDARD_ANALYZER);
        IndexWriter writer = new IndexWriter(directory, config);

        // Add some documents with single-valued numeric fields
        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(new IntPoint("age", i));
            doc.add(new IntPoint("status", i % 10));
            writer.addDocument(doc);
        }

        writer.close();
        reader = DirectoryReader.open(directory);
        when(context.getIndexReader()).thenReturn(reader);

        // Setup numeric field types
        NumberFieldMapper.NumberFieldType intFieldType = mock(NumberFieldMapper.NumberFieldType.class);
        when(intFieldType.numberType()).thenReturn(NumberFieldMapper.NumberType.INTEGER);
        // Make parse return the input value as a Number
        when(intFieldType.parse(any())).thenAnswer(invocation -> {
            Object arg = invocation.getArgument(0);
            if (arg instanceof Number) {
                return (Number) arg;
            }
            return Integer.parseInt(arg.toString());
        });
        when(context.fieldMapper("age")).thenReturn(intFieldType);
        when(context.fieldMapper("status")).thenReturn(intFieldType);
        when(context.fieldMapper("price")).thenReturn(intFieldType);

        // Setup non-numeric field types
        MappedFieldType textFieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("name")).thenReturn(textFieldType);
        when(context.fieldMapper("description")).thenReturn(textFieldType);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        reader.close();
        directory.close();
    }

    public void testRangeQueryRewritten() {
        // Test that must_not range query is rewritten to should clauses
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("type", "product"))
            .mustNot(QueryBuilders.rangeQuery("age").gte(18).lte(65));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have the original term query plus a new bool query from the rewrite
        assertThat(rewrittenBool.must().size(), equalTo(2));

        // The must_not clause should be removed
        assertThat(rewrittenBool.mustNot().size(), equalTo(0));

        // Find the nested bool query
        BoolQueryBuilder nestedBool = null;
        for (QueryBuilder must : rewrittenBool.must()) {
            if (must instanceof BoolQueryBuilder) {
                nestedBool = (BoolQueryBuilder) must;
                break;
            }
        }

        assertNotNull(nestedBool);
        assertThat(nestedBool.should().size(), equalTo(2)); // Two range queries for complement
        assertThat(nestedBool.minimumShouldMatch(), equalTo("1"));
    }

    public void testNumericTermQueryRewritten() {
        // Test that must_not term query on numeric field is rewritten
        QueryBuilder query = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("status", 5));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have a new bool query from the rewrite
        assertThat(rewrittenBool.must().size(), equalTo(1));
        assertThat(rewrittenBool.mustNot().size(), equalTo(0));

        BoolQueryBuilder nestedBool = (BoolQueryBuilder) rewrittenBool.must().get(0);
        assertThat(nestedBool.should().size(), equalTo(2)); // Two range queries for complement
        assertThat(nestedBool.minimumShouldMatch(), equalTo("1"));
    }

    public void testIdempotence() {
        QueryBuilder query = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("status", 3));
        QueryBuilder once = rewriter.rewrite(query, context);
        QueryBuilder twice = rewriter.rewrite(once, context);
        assertEquals(once.toString(), twice.toString());
    }

    public void testMultiValuedNumericFieldNotRewritten() throws Exception {
        // Create an index where a field has multiple values per doc
        Directory multiDir = newDirectory();
        IndexWriter writer = new IndexWriter(multiDir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document doc = new Document();
        doc.add(new IntPoint("multi_age", 10));
        doc.add(new IntPoint("multi_age", 20));
        writer.addDocument(doc);
        writer.close();

        IndexReader multiReader = DirectoryReader.open(multiDir);
        when(context.getIndexReader()).thenReturn(multiReader);
        // numeric field type mapping
        NumberFieldMapper.NumberFieldType intFieldType = mock(NumberFieldMapper.NumberFieldType.class);
        when(context.fieldMapper("multi_age")).thenReturn(intFieldType);

        QueryBuilder query = QueryBuilders.boolQuery().mustNot(QueryBuilders.rangeQuery("multi_age").gte(15));
        QueryBuilder rewritten = rewriter.rewrite(query, context);
        // Should not rewrite because docs can have multiple values
        assertSame(query, rewritten);

        multiReader.close();
        multiDir.close();
    }

    public void testNumericTermsQueryRewritten() {
        // Test that must_not terms query on numeric field is rewritten
        QueryBuilder query = QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery("status", new Object[] { 1, 2, 3 }));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have a new bool query from the rewrite
        assertThat(rewrittenBool.must().size(), equalTo(1));
        assertThat(rewrittenBool.mustNot().size(), equalTo(0));
    }

    public void testNumericMatchQueryRewritten() {
        // Test that must_not match query on numeric field is rewritten
        QueryBuilder query = QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("age", 25));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have a new bool query from the rewrite
        assertThat(rewrittenBool.must().size(), equalTo(1));
        assertThat(rewrittenBool.mustNot().size(), equalTo(0));
    }

    public void testTextFieldNotRewritten() {
        // Test that must_not queries on text fields are not rewritten
        QueryBuilder query = QueryBuilders.boolQuery()
            .mustNot(QueryBuilders.termQuery("name", "test"))
            .mustNot(QueryBuilders.matchQuery("description", "product"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // No changes
    }

    public void testMultipleQueriesOnSameFieldNotRewritten() {
        // Test that multiple must_not queries on the same field are not rewritten
        QueryBuilder query = QueryBuilders.boolQuery()
            .mustNot(QueryBuilders.rangeQuery("age").gte(18))
            .mustNot(QueryBuilders.rangeQuery("age").lte(65));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // No changes
    }

    public void testMinimumShouldMatchHandling() {
        // Test that minimumShouldMatch is properly handled
        QueryBuilder query = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("category", "A"))
            .should(QueryBuilders.termQuery("category", "B"))
            .mustNot(QueryBuilders.rangeQuery("age").gte(18));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Since we added a must clause, minimumShouldMatch should be set to 1
        assertThat(rewrittenBool.minimumShouldMatch(), equalTo("1"));
    }

    public void testExistingMustClausesPreserved() {
        // Test that existing must/filter/should clauses are preserved
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("type", "product"))
            .filter(QueryBuilders.rangeQuery("price").gte(100))
            .should(QueryBuilders.termQuery("featured", true))
            .mustNot(QueryBuilders.rangeQuery("age").gte(18));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Original clauses should be preserved
        assertThat(rewrittenBool.must().size(), equalTo(2)); // Original + rewritten
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        assertThat(rewrittenBool.should().size(), equalTo(1));
        assertThat(rewrittenBool.mustNot().size(), equalTo(0));
    }

    public void testNestedBooleanQueriesRewritten() {
        // Test that nested boolean queries are also rewritten
        QueryBuilder nested = QueryBuilders.boolQuery().mustNot(QueryBuilders.rangeQuery("age").gte(18));

        QueryBuilder query = QueryBuilders.boolQuery().must(nested);

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // The nested bool should be rewritten
        assertThat(rewrittenBool.must().size(), equalTo(1));
        assertThat(rewrittenBool.must().get(0), instanceOf(BoolQueryBuilder.class));

        BoolQueryBuilder innerBool = (BoolQueryBuilder) rewrittenBool.must().get(0);
        assertThat(innerBool.must().size(), equalTo(1)); // The rewritten clause
        assertThat(innerBool.mustNot().size(), equalTo(0));
    }

    public void testNoMustNotClausesNoChanges() {
        // Query without must_not clauses should not be changed
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("type", "product"))
            .filter(QueryBuilders.rangeQuery("price").gte(100));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten);
    }

    public void testNonBoolQueryUnchanged() {
        // Non-bool queries should not be changed
        QueryBuilder query = QueryBuilders.termQuery("field", "value");
        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten);
    }

    public void testNullContextNoRewrite() {
        // With null context, no rewriting should happen
        QueryBuilder query = QueryBuilders.boolQuery().mustNot(QueryBuilders.rangeQuery("age").gte(18));

        QueryBuilder rewritten = rewriter.rewrite(query, null);
        assertSame(query, rewritten);
    }

    public void testRewriterPriority() {
        // Verify rewriter has correct priority
        assertThat(rewriter.priority(), equalTo(175));
        assertThat(rewriter.name(), equalTo("must_not_to_should"));
    }

    public void testBoolQueryPropertiesPreserved() {
        // All bool query properties should be preserved
        QueryBuilder query = QueryBuilders.boolQuery()
            .mustNot(QueryBuilders.rangeQuery("age").gte(18))
            .boost(2.0f)
            .queryName("my_query")
            .adjustPureNegative(false);

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Properties should be preserved
        assertThat(rewrittenBool.boost(), equalTo(2.0f));
        assertThat(rewrittenBool.queryName(), equalTo("my_query"));
        assertThat(rewrittenBool.adjustPureNegative(), equalTo(false));
    }
}
