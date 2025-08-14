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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;

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
        BooleanQuery boolQuery = new BooleanQuery.Builder().add(IntPoint.newRangeQuery("field1", 1, 100), BooleanClause.Occur.FILTER)
            .add(IntPoint.newRangeQuery("field2", 200, 300), BooleanClause.Occur.FILTER)
            .build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

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
                for (int i = 0; i < 100; i++) {
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

    // Test with single clause (nested ApproximateScoreQuery case)
    public void testSingleClauseApproximation() {
        ApproximatePointRangeQuery pointQuery = new ApproximatePointRangeQuery(
            "field",
            new byte[] { 1 },
            new byte[] { 100 },
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

        // Should delegate to nested query's canApproximate
        boolean result = query.canApproximate(mockContext);
        assertTrue(result);
    }

    public void testSingleClauseMustCanApproximate() {
        ApproximateScoreQuery approxQuery = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("field", 1, 100),
            new ApproximatePointRangeQuery("field", new byte[] { 1 }, new byte[] { 100 }, 1, ApproximatePointRangeQuery.INT_FORMAT)
        );

        BooleanQuery boolQuery = new BooleanQuery.Builder().add(approxQuery, BooleanClause.Occur.MUST).build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        // Single clause with MUST should be approximatable with ApproximateScoreQuery
        assertTrue("Single MUST clause with ApproximateScoreQuery should be approximatable", query.canApproximate(mockContext));
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

        // Single clause with SHOULD should be approximatable with ApproximateScoreQuery
        assertTrue("Single SHOULD clause with ApproximateScoreQuery should be approximatable", query.canApproximate(mockContext));
    }

    public void testSingleClauseFilterCanApproximate() {
        ApproximateScoreQuery approxQuery = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("field", 1, 100),
            new ApproximatePointRangeQuery("field", new byte[] { 1 }, new byte[] { 100 }, 1, ApproximatePointRangeQuery.INT_FORMAT)
        );

        BooleanQuery boolQuery = new BooleanQuery.Builder().add(approxQuery, BooleanClause.Occur.FILTER).build();
        ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.highlight()).thenReturn(null);

        // Single clause with FILTER should be approximatable with ApproximateScoreQuery
        assertTrue("Single FILTER clause with ApproximateScoreQuery should be approximatable", query.canApproximate(mockContext));
    }

    // Test BoolQueryBuilder pattern: Single clause WITH ApproximateScoreQuery wrapper
    public void testSingleClauseWithApproximateScoreQueryCanApproximate() {
        // Create ApproximateScoreQuery wrapper (as BoolQueryBuilder would)
        ApproximatePointRangeQuery approxQuery = new ApproximatePointRangeQuery(
            "field",
            new byte[] { 1 },
            new byte[] { 100 },
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
            new byte[] { 1 },
            new byte[] { 100 },
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
            new byte[] { 50 },
            new byte[] { (byte) 150 },
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
                new byte[] { 50 },
                new byte[] { (byte) 150 },
                1,
                ApproximatePointRangeQuery.INT_FORMAT
            )
        );
        ApproximateScoreQuery innerQuery2 = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("inner_field2", 200, 300),
            new ApproximatePointRangeQuery(
                "inner_field2",
                new byte[] { (byte) 200 },
                new byte[] { (byte) 300 },
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
            new ApproximatePointRangeQuery("outer_field", new byte[] { 1 }, new byte[] { 100 }, 1, ApproximatePointRangeQuery.INT_FORMAT)
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
            new ApproximatePointRangeQuery("deep_field1", new byte[] { 1 }, new byte[] { 50 }, 1, ApproximatePointRangeQuery.INT_FORMAT)
        );
        ApproximateScoreQuery deep2Query = new ApproximateScoreQuery(
            IntPoint.newRangeQuery("deep_field2", 51, 100),
            new ApproximatePointRangeQuery("deep_field2", new byte[] { 51 }, new byte[] { 100 }, 1, ApproximatePointRangeQuery.INT_FORMAT)
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
                new byte[] { (byte) 200 },
                new byte[] { (byte) 300 },
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
                new byte[] { (byte) 400 },
                new byte[] { (byte) 500 },
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
                    IndexSearcher searcher = new IndexSearcher(reader);
                    LeafReaderContext leafContext = reader.leaves().get(0);

                    BooleanQuery boolQuery = new BooleanQuery.Builder().add(
                        IntPoint.newRangeQuery("field1", 100, 900),
                        BooleanClause.Occur.FILTER
                    ).add(IntPoint.newRangeQuery("field2", 200, 1800), BooleanClause.Occur.FILTER).build();
                    ApproximateBooleanQuery query = new ApproximateBooleanQuery(boolQuery);

                    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
                    ApproximateBooleanScorerSupplier supplier = (ApproximateBooleanScorerSupplier) weight.scorerSupplier(leafContext);

                    assertNotNull(supplier);

                    // Test that cost calculation works
                    long cost = supplier.cost();
                    assertTrue("Cost should be positive", cost > 0);
                }
            }
        }
    }

    public void testApproximateQueryValidation(
        IndexSearcher searcher,
        String field1,
        String field2,
        int lower1,
        int upper1,
        int lower2,
        int upper2,
        int size
    ) throws IOException {
        // Test with approximate query
        BooleanQuery boolQuery = new BooleanQuery.Builder().add(IntPoint.newRangeQuery(field1, lower1, upper1), BooleanClause.Occur.FILTER)
            .add(IntPoint.newRangeQuery(field2, lower2, upper2), BooleanClause.Occur.FILTER)
            .build();
        ApproximateScoreQuery approxQuery = new ApproximateScoreQuery(boolQuery, new ApproximateBooleanQuery(boolQuery));

        TopDocs approxDocs = searcher.search(approxQuery, size);

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
