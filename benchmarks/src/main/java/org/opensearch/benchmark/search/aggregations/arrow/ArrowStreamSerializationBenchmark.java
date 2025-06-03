/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.search.aggregations.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.util.BytesRef;
import org.opensearch.arrow.flight.stream.ArrowStreamInput;
import org.opensearch.arrow.flight.stream.ArrowStreamOutput;
import org.opensearch.common.logging.NodeNamePatternConverter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class ArrowStreamSerializationBenchmark {

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(
        Arrays.asList(
            new NamedWriteableRegistry.Entry(InternalAggregation.class, StringTerms.NAME, StringTerms::new),
            new NamedWriteableRegistry.Entry(InternalAggregation.class, AvgAggregationBuilder.NAME, InternalAvg::new),
            new NamedWriteableRegistry.Entry(InternalAggregation.class, MaxAggregationBuilder.NAME, InternalMax::new),
            new NamedWriteableRegistry.Entry(InternalAggregation.class, MinAggregationBuilder.NAME, InternalMin::new),
            new NamedWriteableRegistry.Entry(InternalAggregation.class, SumAggregationBuilder.NAME, InternalSum::new),
            new NamedWriteableRegistry.Entry(InternalAggregation.class, ValueCountAggregationBuilder.NAME, InternalValueCount::new),
            new NamedWriteableRegistry.Entry(DocValueFormat.class, DocValueFormat.RAW.getWriteableName(), (si) -> DocValueFormat.RAW)
        )
    );

    @Param(value = { "100", "1000", "5000" })
    private int buckets;

    @Param(value = { "simple_string_terms", "nested_string_terms", "nested_mixed_metrics" })
    private String aggregationType;

    private RootAllocator allocator;
    private InternalAggregations testAggregations;

    @Setup(Level.Trial)
    public void setupAllocator() {
        NodeNamePatternConverter.setNodeName("test");
        allocator = new RootAllocator(Integer.MAX_VALUE);
    }

    @TearDown(Level.Trial)
    public void tearDownAllocator() {
        if (allocator != null) {
            allocator.close();
        }
    }

    @Setup(Level.Iteration)
    public void initResults() {
        switch (aggregationType) {
            case "simple_string_terms":
                testAggregations = InternalAggregations.from(List.of(createSimpleStringTerms()));
                break;
            case "nested_string_terms":
                testAggregations = InternalAggregations.from(List.of(createNestedStringTerms()));
                break;
            case "nested_mixed_metrics":
                testAggregations = InternalAggregations.from(List.of(createNestedMixedMetrics()));
                break;
            default:
                throw new IllegalArgumentException("Unknown aggregation type: " + aggregationType);
        }
    }

    private StringTerms createSimpleStringTerms() {
        List<StringTerms.Bucket> resultBuckets = new ArrayList<>(buckets);
        for (int i = 0; i < buckets; i++) {
            resultBuckets.add(
                new StringTerms.Bucket(new BytesRef("term_" + i), i + 1, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW)
            );
        }
        return new StringTerms(
            "simple_terms",
            BucketOrder.key(true),
            BucketOrder.key(true),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            buckets,
            false,
            100000,
            resultBuckets,
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, buckets, buckets)
        );
    }

    private StringTerms createNestedStringTerms() {
        List<StringTerms.Bucket> resultBuckets = new ArrayList<>(buckets);
        for (int i = 0; i < buckets; i++) {
            // Create nested string terms for each bucket
            List<StringTerms.Bucket> nestedBuckets = new ArrayList<>();
            int nestedCount = Math.min(10, buckets / 10); // Limit nested buckets to avoid explosion
            for (int j = 0; j < nestedCount; j++) {
                // Create third level nested terms
                List<StringTerms.Bucket> thirdLevelBuckets = new ArrayList<>();
                int thirdLevelCount = Math.min(5, nestedCount / 2);
                for (int k = 0; k < thirdLevelCount; k++) {
                    thirdLevelBuckets.add(
                        new StringTerms.Bucket(
                            new BytesRef("level3_term_" + k),
                            k + 1,
                            InternalAggregations.EMPTY,
                            false,
                            0,
                            DocValueFormat.RAW
                        )
                    );
                }

                StringTerms thirdLevelTerms = new StringTerms(
                    "level3_terms",
                    BucketOrder.key(true),
                    BucketOrder.key(true),
                    Collections.emptyMap(),
                    DocValueFormat.RAW,
                    thirdLevelCount,
                    false,
                    1000,
                    thirdLevelBuckets,
                    0,
                    new TermsAggregator.BucketCountThresholds(1, 0, thirdLevelCount, thirdLevelCount)
                );

                nestedBuckets.add(
                    new StringTerms.Bucket(
                        new BytesRef("level2_term_" + j),
                        j + 1,
                        InternalAggregations.from(List.of(thirdLevelTerms)),
                        false,
                        0,
                        DocValueFormat.RAW
                    )
                );
            }

            StringTerms nestedTerms = new StringTerms(
                "level2_terms",
                BucketOrder.key(true),
                BucketOrder.key(true),
                Collections.emptyMap(),
                DocValueFormat.RAW,
                nestedCount,
                false,
                10000,
                nestedBuckets,
                0,
                new TermsAggregator.BucketCountThresholds(1, 0, nestedCount, nestedCount)
            );

            resultBuckets.add(
                new StringTerms.Bucket(
                    new BytesRef("level1_term_" + i),
                    i + 1,
                    InternalAggregations.from(List.of(nestedTerms)),
                    false,
                    0,
                    DocValueFormat.RAW
                )
            );
        }

        return new StringTerms(
            "nested_string_terms",
            BucketOrder.key(true),
            BucketOrder.key(true),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            buckets,
            false,
            100000,
            resultBuckets,
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, buckets, buckets)
        );
    }

    private StringTerms createNestedMixedMetrics() {
        List<StringTerms.Bucket> resultBuckets = new ArrayList<>(buckets);
        for (int i = 0; i < buckets; i++) {
            List<InternalAggregation> metrics = Arrays.asList(
                new InternalAvg("avg_metric", i * 10.5, i + 1, DocValueFormat.RAW, Collections.emptyMap()),
                new InternalMax("max_metric", i * 15.0, DocValueFormat.RAW, Collections.emptyMap()),
                new InternalMin("min_metric", i * 2.0, DocValueFormat.RAW, Collections.emptyMap()),
                new InternalSum("sum_metric", i * 100.0, DocValueFormat.RAW, Collections.emptyMap()),
                new InternalValueCount("count_metric", i + 1, Collections.emptyMap())
            );

            List<StringTerms.Bucket> nestedBuckets = new ArrayList<>();
            int nestedCount = Math.min(5, buckets / 20); // Limit nested buckets
            for (int j = 0; j < nestedCount; j++) {
                List<InternalAggregation> nestedMetrics = Arrays.asList(
                    new InternalAvg("nested_avg", j * 5.5, j + 1, DocValueFormat.RAW, Collections.emptyMap()),
                    new InternalSum("nested_sum", j * 50.0, DocValueFormat.RAW, Collections.emptyMap())
                );

                nestedBuckets.add(
                    new StringTerms.Bucket(
                        new BytesRef("nested_term_" + j),
                        j + 1,
                        InternalAggregations.from(nestedMetrics),
                        false,
                        0,
                        DocValueFormat.RAW
                    )
                );
            }

            StringTerms nestedTerms = new StringTerms(
                "nested_terms_with_metrics",
                BucketOrder.key(true),
                BucketOrder.key(true),
                Collections.emptyMap(),
                DocValueFormat.RAW,
                nestedCount,
                false,
                1000,
                nestedBuckets,
                0,
                new TermsAggregator.BucketCountThresholds(1, 0, nestedCount, nestedCount)
            );

            List<InternalAggregation> allAggregations = new ArrayList<>(metrics);
            allAggregations.add(nestedTerms);

            resultBuckets.add(
                new StringTerms.Bucket(
                    new BytesRef("main_term_" + i),
                    i + 1,
                    InternalAggregations.from(allAggregations),
                    false,
                    0,
                    DocValueFormat.RAW
                )
            );
        }

        return new StringTerms(
            "mixed_metrics_terms",
            BucketOrder.key(true),
            BucketOrder.key(true),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            buckets,
            false,
            100000,
            resultBuckets,
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, buckets, buckets)
        );
    }

    @Benchmark
    public InternalAggregations serializeAndDeserialize() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            testAggregations.writeTo(output);
            VectorSchemaRoot unifiedRoot = output.getUnifiedRoot();
            try (ArrowStreamInput input = new ArrowStreamInput(unifiedRoot, REGISTRY)) {
                return InternalAggregations.readFrom(input);
            }
        }
    }

    @Benchmark
    public VectorSchemaRoot serializeOnly() throws IOException {
        try (ArrowStreamOutput output = new ArrowStreamOutput(allocator)) {
            testAggregations.writeTo(output);
            return output.getUnifiedRoot();
        }
    }
}
