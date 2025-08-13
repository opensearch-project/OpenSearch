/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

/**
 * Benchmark demonstrating query rewriting value.
 * Measures both rewriting overhead and query complexity reduction.
 */
public class QueryRewritingBenchmark extends OpenSearchTestCase {

    private final QueryShardContext context = mock(QueryShardContext.class);
    private static final int WARMUP_ITERATIONS = 1000;
    private static final int BENCHMARK_ITERATIONS = 10000;

    public void testFilterMergingPerformance() {
        // Create query with multiple terms that can be merged
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        String[] brands = { "apple", "samsung", "dell", "hp", "lenovo", "asus", "acer", "msi" };
        for (String brand : brands) {
            query.filter(QueryBuilders.termQuery("brand", brand));
        }

        BenchmarkResult result = runBenchmark("Filter Merging", query);

        // Assert significant node reduction
        assertTrue("Should reduce nodes by > 70%", result.getReductionPercent() > 70);
        assertTrue("Rewrite overhead should be < 50 microseconds", result.rewriteOverhead < 50);
        assertTrue("ROI should be positive", result.getROI() > 1);
    }

    public void testNestedFlatteningPerformance() {
        // Create deeply nested query
        QueryBuilder innerQuery = QueryBuilders.termQuery("status", "active");
        for (int i = 0; i < 5; i++) {
            innerQuery = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).filter(innerQuery);
        }

        BenchmarkResult result = runBenchmark("Nested Flattening", innerQuery);

        // Assert optimization effectiveness
        assertTrue("Should reduce nodes by > 80%", result.getReductionPercent() > 80);
        assertTrue("Rewrite overhead should be < 100 microseconds", result.rewriteOverhead < 100);
    }

    public void testComplexRealWorldQueryPerformance() {
        QueryBuilder query = QueryBuilders.boolQuery()
            // Category filters
            .filter(
                QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("category", "electronics"))
                    .filter(QueryBuilders.termQuery("category", "computers"))
                    .filter(QueryBuilders.termQuery("category", "accessories"))
            )
            // Brand filters
            .filter(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("brand", "apple"))
                    .should(QueryBuilders.termQuery("brand", "samsung"))
                    .should(QueryBuilders.termQuery("brand", "sony"))
                    .should(QueryBuilders.termQuery("brand", "dell"))
                    .should(QueryBuilders.termQuery("brand", "hp"))
            )
            // Availability filters
            .should(QueryBuilders.termQuery("inStock", true))
            .should(QueryBuilders.termQuery("featured", true))
            // Price range
            .must(QueryBuilders.rangeQuery("price").gte(100).lte(2000))
            // Unnecessary match_all
            .must(QueryBuilders.matchAllQuery())
            .filter(QueryBuilders.matchAllQuery());

        BenchmarkResult result = runBenchmark("E-commerce Query", query);

        // Assert real-world optimization value
        assertTrue("Should reduce nodes by > 50%", result.getReductionPercent() > 50);
        assertTrue("Rewrite overhead should be reasonable", result.rewriteOverhead < 200);
        assertTrue("Should provide significant ROI", result.getROI() > 10);
    }

    private BenchmarkResult runBenchmark(String name, QueryBuilder query) {
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            QueryRewriterRegistry.rewrite(query, context, true);
            QueryRewriterRegistry.rewrite(query, context, false);
        }

        // Benchmark without rewriting
        long startWithout = System.nanoTime();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            QueryRewriterRegistry.rewrite(query, context, false);
        }
        long timeWithout = System.nanoTime() - startWithout;

        // Benchmark with rewriting
        long startWith = System.nanoTime();
        QueryBuilder optimized = null;
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            optimized = QueryRewriterRegistry.rewrite(query, context, true);
        }
        long timeWith = System.nanoTime() - startWith;

        int nodesBefore = countNodes(query);
        int nodesAfter = countNodes(optimized);
        double rewriteOverhead = (double) (timeWith - timeWithout) / BENCHMARK_ITERATIONS / 1000; // microseconds

        return new BenchmarkResult(
            name,
            nodesBefore,
            nodesAfter,
            rewriteOverhead,
            timeWith / (double) BENCHMARK_ITERATIONS / 1000,
            timeWithout / (double) BENCHMARK_ITERATIONS / 1000
        );
    }

    private int countNodes(QueryBuilder query) {
        if (query == null || !(query instanceof BoolQueryBuilder)) {
            return 1;
        }
        BoolQueryBuilder bool = (BoolQueryBuilder) query;
        int count = 1;
        count += bool.must().stream().mapToInt(this::countNodes).sum();
        count += bool.filter().stream().mapToInt(this::countNodes).sum();
        count += bool.should().stream().mapToInt(this::countNodes).sum();
        count += bool.mustNot().stream().mapToInt(this::countNodes).sum();
        return count;
    }

    private static class BenchmarkResult {
        final String name;
        final int nodesBefore;
        final int nodesAfter;
        final double rewriteOverhead;
        final double totalTimeWith;
        final double totalTimeWithout;

        BenchmarkResult(
            String name,
            int nodesBefore,
            int nodesAfter,
            double rewriteOverhead,
            double totalTimeWith,
            double totalTimeWithout
        ) {
            this.name = name;
            this.nodesBefore = nodesBefore;
            this.nodesAfter = nodesAfter;
            this.rewriteOverhead = rewriteOverhead;
            this.totalTimeWith = totalTimeWith;
            this.totalTimeWithout = totalTimeWithout;
        }

        double getReductionPercent() {
            return (nodesBefore - nodesAfter) * 100.0 / nodesBefore;
        }

        double getROI() {
            // Assume each node reduction saves 0.5-1ms in execution
            double estimatedSavingsMs = (nodesBefore - nodesAfter) * 0.75;
            double rewriteCostMs = rewriteOverhead / 1000.0;
            return estimatedSavingsMs / rewriteCostMs;
        }
    }
}
