/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.aggregations.BucketCollectorProcessor;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApproximateBooleanQueryTests extends OpenSearchTestCase {

    // Unit Tests for canApproximate method
    public void testCanApproximateWithNullContext() {
        BooleanQuery boolQuery = new BooleanQuery.Builder().add(IntPoint.newRangeQuery("field", 1, 100), BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        assertFalse(query.canApproximate(null));
    }

    public void testCanApproximateWithAccurateTotalHits() {
        BooleanQuery boolQuery = new BooleanQuery.Builder().add(IntPoint.newRangeQuery("field", 1, 100), BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(SearchContext.TRACK_TOTAL_HITS_ACCURATE);

        assertFalse(query.canApproximate(mockContext));
    }

    public void testCanApproximateWithAggregations() {
        BooleanQuery boolQuery = new BooleanQuery.Builder().add(IntPoint.newRangeQuery("field", 1, 100), BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(mock(SearchContextAggregations.class));

        assertFalse(query.canApproximate(mockContext));
    }

    public void testCanApproximateWithHighlighting() {
        BooleanQuery boolQuery = new BooleanQuery.Builder().add(IntPoint.newRangeQuery("field", 1, 100), BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);

        SearchHighlightContext mockHighlight = mock(SearchHighlightContext.class);
        when(mockHighlight.fields()).thenReturn(Arrays.asList(mock(SearchHighlightContext.Field.class)));
        when(mockContext.highlight()).thenReturn(mockHighlight);

        assertFalse(query.canApproximate(mockContext));
    }

    public void testCanApproximateWithValidFilterClauses() {
        ApproximateScoreQuery approxQuery1 = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("field1", 1, 100),
            new ApproximatePointRangeQuery(
                "field1",
                IntPoint.pack(new int[] { 1 }).bytes,
                IntPoint.pack(new int[] { 100 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );
        ApproximateScoreQuery approxQuery2 = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("field2", 200, 300),
            new ApproximatePointRangeQuery(
                "field2",
                IntPoint.pack(new int[] { 200 }).bytes,
                IntPoint.pack(new int[] { 300 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );

        BooleanQuery boolQuery = new BooleanQuery.Builder().add(approxQuery1, BooleanClause.Occur.FILTER)
            .add(approxQuery2, BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        approxQuery1.setContext(mockContext);
        approxQuery2.setContext(mockContext);

        assertTrue(query.canApproximate(mockContext));
    }

    public void testCanApproximateWithMustNotClause() {
        BooleanQuery boolQuery = new BooleanQuery.Builder().add(IntPoint.newRangeQuery("field1", 1, 100), BooleanClause.Occur.FILTER)
            .add(IntPoint.newRangeQuery("field2", 200, 300), BooleanClause.Occur.MUST_NOT)
            .build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        assertFalse(query.canApproximate(mockContext));
    }

    // Unit Tests for ScorerSupplier
    public void testScorerSupplierCreation() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                // Add test documents
                for (int i = 0; i < 20000; i++) {
                    Document doc = new Document();
                    doc.add(new IntPoint("field1", i));
                    doc.add(new IntPoint("field2", i * 2));
                    doc.add(new NumericDocValuesField("field1", i));
                    iw.addDocument(doc);
                }
                iw.flush();

                try (IndexReader reader = iw.getReader()) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    LeafReaderContext leafContext = reader.leaves().get(0);

                    BooleanQuery boolQuery = new BooleanQuery.Builder().add(
                        IntPoint.newRangeQuery("field1", 10, 50),
                        BooleanClause.Occur.FILTER
                    ).add(IntPoint.newRangeQuery("field2", 20, 100), BooleanClause.Occur.FILTER).build();
                    ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

                    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
                    ScorerSupplier supplier = weight.scorerSupplier(leafContext);

                    assertNotNull(supplier);
                    assertTrue(supplier instanceof ApproximateBooleanScorerSupplier);

                    // Test cost estimation
                    assertTrue(supplier.cost() > 0);

                    // Test scorer creation
                    Scorer scorer = supplier.get(1000);
                    assertNotNull(scorer);
                }
            }
        }
    }

    // Integration test comparing approximate vs exact results
    public void testApproximateVsExactResults() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int numDocs = 12000;
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(new IntPoint("field1", i % 1000));
                    doc.add(new IntPoint("field2", (i * 3) % 1000));
                    doc.add(new NumericDocValuesField("field1", i));
                    iw.addDocument(doc);
                }
                iw.flush();

                try (IndexReader reader = iw.getReader()) {
                    IndexSearcher searcher = new IndexSearcher(reader);

                    int lower1 = 200;
                    int upper1 = 400;
                    int lower2 = 300;
                    int upper2 = 500;

                    // Create approximate query
                    ApproximatePointRangeQuery approxQuery1 = new ApproximatePointRangeQuery(
                        "field1",
                        IntPoint.pack(new int[] { lower1 }).bytes,
                        IntPoint.pack(new int[] { upper1 }).bytes,
                        1,
                        ApproximatePointRangeQuery.INT_FORMAT
                    );
                    ApproximatePointRangeQuery approxQuery2 = new ApproximatePointRangeQuery(
                        "field2",
                        IntPoint.pack(new int[] { lower2 }).bytes,
                        IntPoint.pack(new int[] { upper2 }).bytes,
                        1,
                        ApproximatePointRangeQuery.INT_FORMAT
                    );

                    BooleanQuery boolQuery = new BooleanQuery.Builder().add(approxQuery1, BooleanClause.Occur.FILTER)
                        .add(approxQuery2, BooleanClause.Occur.FILTER)
                        .build();

                    ApproximateBooleanQuery approximateQuery = new ApproximateBooleanQuery(boolQuery);

                    // Create exact query (same boolean structure)
                    Query exactQuery = boolQuery;

                    // Search with both queries
                    TopDocs approximateDocs = searcher.search(approximateQuery, 1000);
                    TopDocs exactDocs = searcher.search(exactQuery, 1000);

                    System.out.println("Exact docs total hits: " + exactDocs.totalHits.value());
                    System.out.println("Approx docs total hits: " + approximateDocs.totalHits.value());

                    // Results should be identical when approximation is not triggered
                    // or when we collect all available documents
                    if (exactDocs.totalHits.value() <= 1000) {
                        assertEquals(
                            "Approximate and exact should return same number of docs when under limit",
                            exactDocs.totalHits.value(),
                            approximateDocs.totalHits.value()
                        );
                    }
                }
            }
        }
    }

    // Test with single clause (nested ApproximateScoreQuery case)
    public void testSingleClauseApproximation() {
        ApproximatePointRangeQuery pointQuery = new ApproximatePointRangeQuery(
            "field",
            IntPoint.pack(new int[] { 1 }).bytes,
            IntPoint.pack(new int[] { 100 }).bytes,
            1,
            ApproximatePointRangeQuery.LONG_FORMAT
        );
        ApproximateScoreQuery scoreQuery = new ApproximateScoreQuery(IntPoint.newRangeQuery("field", 1, 100), pointQuery);

        BooleanQuery boolQuery = new BooleanQuery.Builder().add(scoreQuery, BooleanClause.Occur.MUST).build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        scoreQuery.setContext(mockContext);

        // Should delegate to nested query's canApproximate
        boolean result = query.canApproximate(mockContext);
        assertTrue(result);
    }

    // Test BoolQueryBuilder pattern: All FILTER clauses (multi-clause)
    public void testAllFilterClausesCanApproximate() {
        // Create approximatable range queries manually
        ApproximateScoreQuery approxQuery1 = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("field1", 1, 100),
            new ApproximatePointRangeQuery(
                "field1",
                IntPoint.pack(new int[] { 1 }).bytes,
                IntPoint.pack(new int[] { 100 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );
        ApproximateScoreQuery approxQuery2 = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("field2", 200, 300),
            new ApproximatePointRangeQuery(
                "field2",
                IntPoint.pack(new int[] { 200 }).bytes,
                IntPoint.pack(new int[] { 300 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );
        ApproximateScoreQuery approxQuery3 = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("field3", 400, 500),
            new ApproximatePointRangeQuery(
                "field3",
                IntPoint.pack(new int[] { 400 }).bytes,
                IntPoint.pack(new int[] { 500 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );

        BooleanQuery boolQuery = new BooleanQuery.Builder().add(approxQuery1, BooleanClause.Occur.FILTER)
            .add(approxQuery2, BooleanClause.Occur.FILTER)
            .add(approxQuery3, BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        approxQuery1.setContext(mockContext);
        approxQuery2.setContext(mockContext);
        approxQuery3.setContext(mockContext);

        assertTrue("All FILTER clauses should be approximatable", query.canApproximate(mockContext));
    }

    public void testSingleClauseMustCanApproximate() {
        BooleanQuery boolQuery = new BooleanQuery.Builder().add(IntPoint.newRangeQuery("field", 1, 100), BooleanClause.Occur.MUST).build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        // Single clause with MUST should return false (not handled by current logic)
        assertFalse("Single MUST clause should not be approximatable", query.canApproximate(mockContext));
    }

    public void testSingleClauseShouldCanApproximate() {
        ApproximateScoreQuery approxQuery = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("field", 1, 100),
            new ApproximatePointRangeQuery("field", new byte[] { 1 }, new byte[] { 100 }, 1, ApproximatePointRangeQuery.INT_FORMAT)
        );

        BooleanQuery boolQuery = new BooleanQuery.Builder().add(approxQuery, BooleanClause.Occur.SHOULD).build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        approxQuery.setContext(mockContext);

        // Single clause with SHOULD should be approximatable with ApproximateScoreQuery
        assertTrue("Single SHOULD clause with ApproximateScoreQuery should be approximatable", query.canApproximate(mockContext));
    }

    public void testSingleClauseFilterCanApproximate() {
        BooleanQuery boolQuery = new BooleanQuery.Builder().add(IntPoint.newRangeQuery("field", 1, 100), BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        // Single clause with FILTER should return false (not MUST_NOT, but not handled)
        assertFalse("Single FILTER clause should not be approximatable", query.canApproximate(mockContext));
    }

    // Test BoolQueryBuilder pattern: Single clause WITH ApproximateScoreQuery wrapper
    public void testSingleClauseWithApproximateScoreQueryCanApproximate() {
        // Create ApproximateScoreQuery wrapper (as BoolQueryBuilder would)
        ApproximatePointRangeQuery approxQuery = new ApproximatePointRangeQuery(
            "field",
            IntPoint.pack(new int[] { 1 }).bytes,
            IntPoint.pack(new int[] { 100 }).bytes,
            1,
            ApproximatePointRangeQuery.LONG_FORMAT
        );
        ApproximateScoreQuery scoreQuery = new ApproximateScoreQuery(IntPoint.newRangeQuery("field", 1, 100), approxQuery);

        // Test all single clause types (MUST, SHOULD, FILTER) - all should work
        BooleanClause.Occur[] occurs = { BooleanClause.Occur.MUST, BooleanClause.Occur.SHOULD, BooleanClause.Occur.FILTER };

        for (BooleanClause.Occur occur : occurs) {
            BooleanQuery boolQuery = new BooleanQuery.Builder().add(scoreQuery, occur).build();
            ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

            SearchContext mockContext = mock(SearchContext.class);
            when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
            when(mockContext.aggregations()).thenReturn(null);
            when(mockContext.highlight()).thenReturn(null);

            // Single clause with ApproximateScoreQuery should delegate to nested query
            boolean result = query.canApproximate(mockContext);
            assertTrue("Single " + occur + " clause with ApproximateScoreQuery should be approximatable", result);
        }
    }

    // Test single MUST_NOT clause should NOT be approximatable
    public void testSingleClauseMustNotCannotApproximate() {
        ApproximatePointRangeQuery approxQuery = new ApproximatePointRangeQuery(
            "field",
            IntPoint.pack(new int[] { 1 }).bytes,
            IntPoint.pack(new int[] { 100 }).bytes,
            1,
            ApproximatePointRangeQuery.LONG_FORMAT
        );
        ApproximateScoreQuery scoreQuery = new ApproximateScoreQuery(IntPoint.newRangeQuery("field", 1, 100), approxQuery);

        BooleanQuery boolQuery = new BooleanQuery.Builder().add(scoreQuery, BooleanClause.Occur.MUST_NOT).build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        // Single MUST_NOT clause should be blocked
        assertFalse("Single MUST_NOT clause should not be approximatable", query.canApproximate(mockContext));
    }

    public void testNestedSingleClauseWithApproximateScoreQuery() {
        // Create inner ApproximateScoreQuery manually (verbose version)
        ApproximatePointRangeQuery innerApproxQuery = new ApproximatePointRangeQuery(
            "inner_field",
            IntPoint.pack(new int[] { 50 }).bytes,
            IntPoint.pack(new int[] { 150 }).bytes,
            1,
            ApproximatePointRangeQuery.INT_FORMAT
        );
        ApproximateScoreQuery innerScoreQuery = new ApproximateScoreQuery(IntPoint.newRangeQuery("inner_field", 50, 150), innerApproxQuery);

        // Inner boolean query (single clause)
        BooleanQuery innerBoolQuery = new BooleanQuery.Builder().add(innerScoreQuery, BooleanClause.Occur.FILTER).build();

        ApproximateBooleanQuery innerApproxBoolQuery = new ApproximateBooleanQuery(innerBoolQuery);
        ApproximateScoreQuery outerScoreQuery = new ApproximateScoreQuery(innerBoolQuery, innerApproxBoolQuery);

        // Outer boolean query (single clause containing nested)
        BooleanQuery outerBoolQuery = new BooleanQuery.Builder().add(outerScoreQuery, BooleanClause.Occur.MUST).build();
        ApproximateBooleanQuery outerQuery = new ApproximateBooleanQuery(outerBoolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        // Should delegate to nested ApproximateBooleanQuery
        boolean result = outerQuery.canApproximate(mockContext);
        assertTrue("Nested single clause should follow inner query logic and be approximatable", result);
    }

    // Test nested boolean query with ApproximateScoreQuery wrapper (multi-clause pattern)
    public void testNestedMultiClauseWithApproximateScoreQuery() {
        // Create inner ApproximateScoreQuery instances manually
        ApproximateScoreQuery innerQuery1 = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("inner_field1", 50, 150),
            new ApproximatePointRangeQuery(
                "inner_field1",
                IntPoint.pack(new int[] { 50 }).bytes,
                IntPoint.pack(new int[] { 150 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );
        ApproximateScoreQuery innerQuery2 = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("inner_field2", 200, 300),
            new ApproximatePointRangeQuery(
                "inner_field2",
                IntPoint.pack(new int[] { 200 }).bytes,
                IntPoint.pack(new int[] { 300 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );

        // Inner boolean query (all FILTER clauses)
        BooleanQuery innerBoolQuery = new BooleanQuery.Builder().add(innerQuery1, BooleanClause.Occur.FILTER)
            .add(innerQuery2, BooleanClause.Occur.FILTER)
            .build();

        ApproximateBooleanQuery innerApproxQuery = new ApproximateBooleanQuery(innerBoolQuery);
        ApproximateScoreQuery scoreQuery = new ApproximateScoreQuery(innerBoolQuery, innerApproxQuery);

        // Create outer ApproximateScoreQuery manually
        ApproximateScoreQuery outerFieldQuery = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("outer_field", 1, 100),
            new ApproximatePointRangeQuery(
                "outer_field",
                IntPoint.pack(new int[] { 1 }).bytes,
                IntPoint.pack(new int[] { 100 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );

        // Outer boolean query (multi-clause with nested)
        BooleanQuery outerBoolQuery = new BooleanQuery.Builder().add(scoreQuery, BooleanClause.Occur.FILTER)
            .add(outerFieldQuery, BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery outerQuery = new ApproximateBooleanQuery(outerBoolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        // Should delegate to nested ApproximateBooleanQuery and return true
        assertFalse("Nested multi-FILTER clause should not be approximatable", outerQuery.canApproximate(mockContext));
    }

    // Test mixed clause types (should not be approximatable)
    public void testMixedClauseTypesCannotApproximate() {
        BooleanQuery boolQuery = new BooleanQuery.Builder().add(IntPoint.newRangeQuery("field1", 1, 100), BooleanClause.Occur.FILTER)
            .add(IntPoint.newRangeQuery("field2", 200, 300), BooleanClause.Occur.MUST)
            .add(IntPoint.newRangeQuery("field3", 400, 500), BooleanClause.Occur.SHOULD)
            .build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        assertFalse("Mixed clause types should not be approximatable", query.canApproximate(mockContext));
    }

    // Test deeply nested boolean queries
    public void testDeeplyNestedBooleanQueries() {
        // Level 3 (deepest) - Create ApproximateScoreQuery manually
        ApproximateScoreQuery deep1Query = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("deep_field1", 1, 50),
            new ApproximatePointRangeQuery(
                "deep_field1",
                IntPoint.pack(new int[] { 1 }).bytes,
                IntPoint.pack(new int[] { 50 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );
        ApproximateScoreQuery deep2Query = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("deep_field2", 51, 100),
            new ApproximatePointRangeQuery(
                "deep_field2",
                IntPoint.pack(new int[] { 51 }).bytes,
                IntPoint.pack(new int[] { 100 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );

        BooleanQuery level3Query = new BooleanQuery.Builder().add(deep1Query, BooleanClause.Occur.FILTER)
            .add(deep2Query, BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery level3Approx = new ApproximateBooleanQuery(level3Query);
        ApproximateScoreQuery level3Score = new ApproximateScoreQuery(level3Query, level3Approx);

        // Level 2 (middle)
        ApproximateScoreQuery midQuery = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("mid_field", 200, 300),
            new ApproximatePointRangeQuery(
                "mid_field",
                IntPoint.pack(new int[] { 200 }).bytes,
                IntPoint.pack(new int[] { 300 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );

        BooleanQuery level2Query = new BooleanQuery.Builder().add(level3Score, BooleanClause.Occur.FILTER)
            .add(midQuery, BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery level2Approx = new ApproximateBooleanQuery(level2Query);
        ApproximateScoreQuery level2Score = new ApproximateScoreQuery(level2Query, level2Approx);

        // Level 1 (top)
        ApproximateScoreQuery topFieldQuery = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("top_field", 400, 500),
            new ApproximatePointRangeQuery(
                "top_field",
                IntPoint.pack(new int[] { 400 }).bytes,
                IntPoint.pack(new int[] { 500 }).bytes,
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );

        BooleanQuery level1Query = new BooleanQuery.Builder().add(level2Score, BooleanClause.Occur.FILTER)
            .add(topFieldQuery, BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery topQuery = new ApproximateBooleanQuery(level1Query);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        assertFalse("Deeply nested all-FILTER queries should not be approximatable", topQuery.canApproximate(mockContext));
    }

    // Test edge case: nested query with highlighting should be blocked
    public void testNestedQueryWithHighlightingBlocked() {
        // Inner boolean query (all FILTER clauses)
        BooleanQuery innerBoolQuery = new BooleanQuery.Builder().add(
            IntPoint.newRangeQuery("inner_field1", 50, 150),
            BooleanClause.Occur.FILTER
        ).add(IntPoint.newRangeQuery("inner_field2", 200, 300), BooleanClause.Occur.FILTER).build();

        ApproximateBooleanQuery innerApproxQuery = new ApproximateBooleanQuery(innerBoolQuery);
        ApproximateScoreQuery scoreQuery = new ApproximateScoreQuery(innerBoolQuery, innerApproxQuery);

        // Outer boolean query
        BooleanQuery outerBoolQuery = new BooleanQuery.Builder().add(scoreQuery, BooleanClause.Occur.FILTER).build();
        ApproximateBooleanQuery outerQuery = new ApproximateBooleanQuery(outerBoolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);

        // Add highlighting
        SearchHighlightContext mockHighlight = mock(SearchHighlightContext.class);
        when(mockHighlight.fields()).thenReturn(Arrays.asList(mock(SearchHighlightContext.Field.class)));
        when(mockContext.highlight()).thenReturn(mockHighlight);

        assertFalse("Nested queries with highlighting should be blocked", outerQuery.canApproximate(mockContext));
    }

    // Test edge case: nested query with one level having MUST_NOT
    public void testNestedQueryWithMustNotClause() {
        // Inner boolean query (contains MUST_NOT)
        BooleanQuery innerBoolQuery = new BooleanQuery.Builder().add(
            IntPoint.newRangeQuery("inner_field1", 50, 150),
            BooleanClause.Occur.FILTER
        ).add(IntPoint.newRangeQuery("inner_field2", 200, 300), BooleanClause.Occur.MUST_NOT).build();

        ApproximateBooleanQuery innerApproxQuery = new ApproximateBooleanQuery(innerBoolQuery);
        ApproximateScoreQuery scoreQuery = new ApproximateScoreQuery(innerBoolQuery, innerApproxQuery);

        // Outer boolean query (all FILTER)
        BooleanQuery outerBoolQuery = new BooleanQuery.Builder().add(scoreQuery, BooleanClause.Occur.FILTER)
            .add(IntPoint.newRangeQuery("outer_field", 1, 100), BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery outerQuery = new ApproximateBooleanQuery(outerBoolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        // Should be blocked by inner MUST_NOT clause
        assertFalse("Nested query with MUST_NOT should not be approximatable", outerQuery.canApproximate(mockContext));
    }

    // Test BulkScorer with large dataset to trigger windowed expansion
    // public void testBulkScorerWindowedExpansion() throws IOException {
    // try (Directory directory = newDirectory()) {
    // try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
    // int numDocs = 20000;
    // for (int i = 0; i < numDocs; i++) {
    // Document doc = new Document();
    // doc.add(new IntPoint("field1", i));
    // doc.add(new IntPoint("field2", i % 1000)); // Create dense overlapping ranges
    // doc.add(new NumericDocValuesField("field1", i));
    // doc.add(new NumericDocValuesField("field2", i % 1000));
    // doc.add(new StoredField("field1", i));
    // doc.add(new StoredField("field2", i % 1000));
    // iw.addDocument(doc);
    // }
    // iw.flush();
    //
    // try (IndexReader reader = iw.getReader()) {
    // ContextIndexSearcher searcher = createContextIndexSearcher(reader);
    //
    // // Create approximate queries directly
    // ApproximatePointRangeQuery approxQuery1 = new ApproximatePointRangeQuery(
    // "field1",
    // IntPoint.pack(new int[] { 1000 }).bytes,
    // IntPoint.pack(new int[] { 20000 }).bytes,
    // 1,
    // ApproximatePointRangeQuery.INT_FORMAT
    // );
    // ApproximatePointRangeQuery approxQuery2 = new ApproximatePointRangeQuery(
    // "field2",
    // IntPoint.pack(new int[] { 100 }).bytes,
    // IntPoint.pack(new int[] { 900 }).bytes,
    // 1,
    // ApproximatePointRangeQuery.INT_FORMAT
    // );
    //
    // BooleanQuery boolQuery = new BooleanQuery.Builder().add(approxQuery1, BooleanClause.Occur.FILTER)
    // .add(approxQuery2, BooleanClause.Occur.FILTER)
    // .build();
    // ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);
    //
    // TopScoreDocCollector collector = new TopScoreDocCollectorManager(10001, 10001).newCollector();
    // searcher.search(query, collector);
    // TopDocs docs = collector.topDocs();
    //
    // System.out.println("ScoreDocs length: "+docs.scoreDocs.length);
    // System.out.println("total hits value" + docs.totalHits.value());
    // // Should collect documents and potentially expand windows
    // assertTrue("Should collect some documents", docs.scoreDocs.length > 0);
    // assertTrue("Should collect up to 10k documents or exhaust", docs.scoreDocs.length <= 10001);
    // }
    // }
    // }
    // }

    /**
     * Creates a ContextIndexSearcher with properly mocked SearchContext for testing.
     */
    private ContextIndexSearcher createContextIndexSearcher(IndexReader reader) throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(searchContext.indexShard()).thenReturn(indexShard);
        SearchOperationListener searchOperationListener = new SearchOperationListener() {
        };
        when(indexShard.getSearchOperationListener()).thenReturn(searchOperationListener);
        when(searchContext.bucketCollectorProcessor()).thenReturn(new BucketCollectorProcessor());
        when(searchContext.asLocalBucketCountThresholds(any())).thenCallRealMethod();

        ContextIndexSearcher searcher = new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            mock(ExecutorService.class),
            searchContext
        );

        searcher.addQueryCancellation(() -> {});
        return searcher;
    }

    // // Integration test validating hit count and accuracy
    // public void testApproximateResultsValidation() throws IOException {
    // try (Directory directory = newDirectory()) {
    // try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
    // int numDocs = 20000;
    // for (int i = 0; i < numDocs; i++) {
    // Document doc = new Document();
    // int field1Value = i % 1000; // Values: 0-999 (1000 unique values)
    // int field2Value = i % 500; // Values: 0-499 (500 unique values)
    // doc.add(new IntPoint("field1", field1Value));
    // doc.add(new IntPoint("field2", field2Value));
    // doc.add(new NumericDocValuesField("field1", field1Value));
    // doc.add(new NumericDocValuesField("field2", field2Value));
    // doc.add(new StoredField("field1", field1Value));
    // doc.add(new StoredField("field2", field2Value));
    // iw.addDocument(doc);
    // }
    // iw.flush();
    //
    // try (IndexReader reader = iw.getReader()) {
    // ContextIndexSearcher searcher = createContextIndexSearcher(reader);
    //
    // int lower1 = 100;
    // int upper1 = 200;
    // int lower2 = 50;
    // int upper2 = 150;
    //
    // // Create approximate query
    // ApproximatePointRangeQuery approxQuery1 = new ApproximatePointRangeQuery(
    // "field1",
    // IntPoint.pack(new int[] { lower1 }).bytes,
    // IntPoint.pack(new int[] { upper1 }).bytes,
    // 1,
    // ApproximatePointRangeQuery.INT_FORMAT
    // );
    // ApproximatePointRangeQuery approxQuery2 = new ApproximatePointRangeQuery(
    // "field2",
    // IntPoint.pack(new int[] { lower2 }).bytes,
    // IntPoint.pack(new int[] { upper2 }).bytes,
    // 1,
    // ApproximatePointRangeQuery.INT_FORMAT
    // );
    //
    // BooleanQuery approximateBoolQuery = new BooleanQuery.Builder().add(approxQuery1, BooleanClause.Occur.FILTER)
    // .add(approxQuery2, BooleanClause.Occur.FILTER)
    // .build();
    // ApproximateBooleanQuery approximateQuery = new ApproximateBooleanQuery(approximateBoolQuery);
    //
    // // Create exact query (regular Lucene BooleanQuery)
    // BooleanQuery exactBoolQuery = new BooleanQuery.Builder().add(
    // IntPoint.newRangeQuery("field1", lower1, upper1),
    // BooleanClause.Occur.FILTER
    // ).add(IntPoint.newRangeQuery("field2", lower2, upper2), BooleanClause.Occur.FILTER).build();
    //
    // TopScoreDocCollector collector = new TopScoreDocCollectorManager(10001, 10001).newCollector();
    //
    // searcher.search(approximateQuery, collector);
    //
    // // Search with both queries
    // TopDocs approximateDocs = collector.topDocs();
    //
    // TopScoreDocCollector collectorExact = new TopScoreDocCollectorManager(10001, 10001).newCollector();
    //
    // searcher.search(exactBoolQuery, collectorExact);
    //
    // // Search with both queries
    // TopDocs exactDocs = collectorExact.topDocs();
    //
    // System.out.println("Exact hits: " + exactDocs.totalHits.value());
    // System.out.println("Approximate hits: " + approximateDocs.totalHits.value());
    // System.out.println("approximate score docs length: " + approximateDocs.scoreDocs.length);
    // // Validate hit count logic
    // if (exactDocs.totalHits.value() <= 10000) {
    // assertEquals(
    // "When exact results ≤ 10k, approximate should match exactly",
    // exactDocs.totalHits.value(),
    // approximateDocs.totalHits.value()
    // );
    // } else {
    // assertEquals(
    // "Approximate should return exactly 10k hits when exact > 10k",
    // 10000,
    // approximateDocs.totalHits.value()
    // );
    // }
    //
    // // Validate hit accuracy - each returned doc should match the query criteria
    // StoredFields storedFields = reader.storedFields();
    // for (int i = 0; i < approximateDocs.scoreDocs.length; i++) {
    // int docId = approximateDocs.scoreDocs[i].doc;
    // Document doc = storedFields.document(docId);
    //
    // int field1Value = doc.getField("field1").numericValue().intValue();
    // int field2Value = doc.getField("field2").numericValue().intValue();
    //
    // assertTrue(
    // "field1 should be in range [" + lower1 + ", " + upper1 + "], got: " + field1Value,
    // field1Value >= lower1 && field1Value <= upper1
    // );
    // assertTrue(
    // "field2 should be in range [" + lower2 + ", " + upper2 + "], got: " + field2Value,
    // field2Value >= lower2 && field2Value <= upper2
    // );
    // }
    // }
    // }
    // }
    // }

    // Test window size heuristic with different cost scenarios
    public void testWindowSizeHeuristic() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                for (int i = 0; i < 1000; i++) {
                    Document doc = new Document();
                    doc.add(new IntPoint("field1", i));
                    doc.add(new IntPoint("field2", i * 2));
                    iw.addDocument(doc);
                }
                iw.flush();

                try (IndexReader reader = iw.getReader()) {
                    ContextIndexSearcher searcher = createContextIndexSearcher(reader);
                    LeafReaderContext leafContext = reader.leaves().get(0);

                    // Create approximate queries directly
                    ApproximatePointRangeQuery approxQuery1 = new ApproximatePointRangeQuery(
                        "field1",
                        IntPoint.pack(new int[] { 100 }).bytes,
                        IntPoint.pack(new int[] { 900 }).bytes,
                        1,
                        ApproximatePointRangeQuery.INT_FORMAT
                    );
                    ApproximatePointRangeQuery approxQuery2 = new ApproximatePointRangeQuery(
                        "field2",
                        IntPoint.pack(new int[] { 200 }).bytes,
                        IntPoint.pack(new int[] { 1800 }).bytes,
                        1,
                        ApproximatePointRangeQuery.INT_FORMAT
                    );

                    BooleanQuery boolQuery = new BooleanQuery.Builder().add(approxQuery1, BooleanClause.Occur.FILTER)
                        .add(approxQuery2, BooleanClause.Occur.FILTER)
                        .build();
                    ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

                    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
                    ApproximateBooleanScorerSupplier supplier = (ApproximateBooleanScorerSupplier) weight.scorerSupplier(leafContext);

                    assertNotNull(supplier);
                }
            }
        }
    }

    // Test sparse data distribution (simulating http_logs dataset)
    public void testSparseDataDistribution() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                String fieldName1 = "timestamp";
                String fieldName2 = "status_code";

                // Create sparse timestamp distribution with dense status code clusters
                for (int i = 0; i < 10000; i++) {
                    Document doc = new Document();
                    // Sparse timestamps (gaps in time)
                    int timestamp = i * 10 + (i % 6);
                    // Dense status code clusters (200s, 400s, 500s)
                    int statusCode = (i % 100) < 70 ? 200 + (i % 11) : ((i % 100) < 80 ? 400 + (i % 11) : 500 + (i % 11));

                    doc.add(new IntPoint(fieldName1, timestamp));
                    doc.add(new IntPoint(fieldName2, statusCode));
                    doc.add(new NumericDocValuesField(fieldName1, timestamp));
                    doc.add(new NumericDocValuesField(fieldName2, statusCode));
                    doc.add(new StoredField(fieldName1, timestamp));
                    doc.add(new StoredField(fieldName2, statusCode));
                    iw.addDocument(doc);
                }
                iw.flush();

                try (IndexReader reader = iw.getReader()) {
                    ContextIndexSearcher searcher = createContextIndexSearcher(reader);

                    // Test query for specific time range and status codes
                    testApproximateQueryValidation(searcher, fieldName1, fieldName2, 10000, 50000, 200, 500, 100);
                    testApproximateQueryValidation(searcher, fieldName1, fieldName2, 0, 20000, 404, 404, 50);
                }
            }
        }
    }

    // Test dense data distribution (simulating nyc_taxis dataset)
    public void testDenseDataDistribution() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                String fieldName1 = "fare_amount";
                String fieldName2 = "trip_distance";

                // Create dense overlapping distributions
                for (int fare = 500; fare <= 5000; fare += 50) { // Dense fare distribution
                    for (int distance = 1; distance <= 50; distance += 2) { // Dense distance distribution
                        // Add multiple documents per combination to create density
                        int numDocs = 3;
                        for (int d = 0; d < numDocs; d++) {
                            Document doc = new Document();
                            doc.add(new IntPoint(fieldName1, fare));
                            doc.add(new IntPoint(fieldName2, distance));
                            doc.add(new NumericDocValuesField(fieldName1, fare));
                            doc.add(new NumericDocValuesField(fieldName2, distance));
                            doc.add(new StoredField(fieldName1, fare));
                            doc.add(new StoredField(fieldName2, distance));
                            iw.addDocument(doc);
                        }
                    }
                }
                iw.flush();

                try (IndexReader reader = iw.getReader()) {
                    ContextIndexSearcher searcher = createContextIndexSearcher(reader);

                    // Test queries for different fare and distance ranges
                    testApproximateQueryValidation(searcher, fieldName1, fieldName2, 1000, 3000, 5, 25, 200);
                    testApproximateQueryValidation(searcher, fieldName1, fieldName2, 2000, 4000, 10, 40, 500);
                }
            }
        }
    }

    public void testApproximateQueryValidation(
        ContextIndexSearcher searcher,
        String field1,
        String field2,
        int lower1,
        int upper1,
        int lower2,
        int upper2,
        int size
    ) throws IOException {
        // Create approximate query using ApproximatePointRangeQuery directly
        ApproximatePointRangeQuery approxQuery1 = new ApproximatePointRangeQuery(
            field1,
            IntPoint.pack(new int[] { lower1 }).bytes,
            IntPoint.pack(new int[] { upper1 }).bytes,
            1,
            ApproximatePointRangeQuery.INT_FORMAT
        );
        ApproximatePointRangeQuery approxQuery2 = new ApproximatePointRangeQuery(
            field2,
            IntPoint.pack(new int[] { lower2 }).bytes,
            IntPoint.pack(new int[] { upper2 }).bytes,
            1,
            ApproximatePointRangeQuery.INT_FORMAT
        );

        BooleanQuery boolQuery = new BooleanQuery.Builder().add(approxQuery1, BooleanClause.Occur.FILTER)
            .add(approxQuery2, BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery approximateQuery = new ApproximateBooleanQuery(boolQuery);

        TopScoreDocCollector collector = new TopScoreDocCollectorManager(size + 1, size + 1).newCollector();
        searcher.search(approximateQuery, collector);
        TopDocs approxDocs = collector.topDocs();

        // Validate hit count
        assertTrue("Approximate query should return at most " + size + " docs", approxDocs.scoreDocs.length <= size);
        assertTrue("Should not exceed 10k hits", approxDocs.totalHits.value() <= 10000);

        // Validate hit accuracy - each returned doc should match the query criteria
        StoredFields storedFields = searcher.getIndexReader().storedFields();
        for (int i = 0; i < approxDocs.scoreDocs.length; i++) {
            int docId = approxDocs.scoreDocs[i].doc;
            Document doc = storedFields.document(docId);

            int field1Value = doc.getField(field1).numericValue().intValue();
            int field2Value = doc.getField(field2).numericValue().intValue();

            assertTrue(
                field1 + " should be in range [" + lower1 + ", " + upper1 + "], got: " + field1Value,
                field1Value >= lower1 && field1Value <= upper1
            );
            assertTrue(
                field2 + " should be in range [" + lower2 + ", " + upper2 + "], got: " + field2Value,
                field2Value >= lower2 && field2Value <= upper2
            );
        }
    }
}
