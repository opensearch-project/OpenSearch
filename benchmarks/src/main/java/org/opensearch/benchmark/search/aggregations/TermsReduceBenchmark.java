/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.benchmark.search.aggregations;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.QueryPhaseResultConsumer;
import org.opensearch.action.search.SearchPhaseController;
import org.opensearch.action.search.SearchProgressListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchModule;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregator;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.query.QuerySearchResult;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

@Warmup(iterations = 5)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class TermsReduceBenchmark {
    private final SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
    private final SearchPhaseController controller = new SearchPhaseController(
        namedWriteableRegistry,
        req -> new InternalAggregation.ReduceContextBuilder() {
            @Override
            public InternalAggregation.ReduceContext forPartialReduction() {
                return InternalAggregation.ReduceContext.forPartialReduction(null, null, () -> PipelineAggregator.PipelineTree.EMPTY);
            }

            @Override
            public InternalAggregation.ReduceContext forFinalReduction() {
                final MultiBucketConsumerService.MultiBucketConsumer bucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
                    Integer.MAX_VALUE,
                    new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                );
                return InternalAggregation.ReduceContext.forFinalReduction(
                    null,
                    null,
                    bucketConsumer,
                    PipelineAggregator.PipelineTree.EMPTY
                );
            }
        }
    );

    @State(Scope.Benchmark)
    public static class TermsList extends AbstractList<InternalAggregations> {
        @Param({ "1600172297" })
        long seed;

        @Param({ "64", "128", "512" })
        int numShards;

        @Param({ "100" })
        int topNSize;

        @Param({ "1", "10", "100" })
        int cardinalityFactor;

        List<InternalAggregations> aggsList;

        @Setup
        public void setup() {
            this.aggsList = new ArrayList<>();
            Random rand = new Random(seed);
            int cardinality = cardinalityFactor * topNSize;
            BytesRef[] dict = new BytesRef[cardinality];
            for (int i = 0; i < dict.length; i++) {
                dict[i] = new BytesRef(Long.toString(rand.nextLong()));
            }
            for (int i = 0; i < numShards; i++) {
                aggsList.add(InternalAggregations.from(Collections.singletonList(newTerms(rand, dict, true))));
            }
        }

        private StringTerms newTerms(Random rand, BytesRef[] dict, boolean withNested) {
            Set<BytesRef> randomTerms = new HashSet<>();
            for (int i = 0; i < topNSize; i++) {
                randomTerms.add(dict[rand.nextInt(dict.length)]);
            }
            List<StringTerms.Bucket> buckets = new ArrayList<>();
            for (BytesRef term : randomTerms) {
                InternalAggregations subAggs;
                if (withNested) {
                    subAggs = InternalAggregations.from(Collections.singletonList(newTerms(rand, dict, false)));
                } else {
                    subAggs = InternalAggregations.EMPTY;
                }
                buckets.add(new StringTerms.Bucket(term, rand.nextInt(10000), subAggs, true, 0L, DocValueFormat.RAW));
            }

            Collections.sort(buckets, (a, b) -> a.compareKey(b));
            return new StringTerms(
                "terms",
                BucketOrder.key(true),
                BucketOrder.count(false),
                Collections.emptyMap(),
                DocValueFormat.RAW,
                numShards,
                true,
                0,
                buckets,
                0,
                new TermsAggregator.BucketCountThresholds(1, 0, topNSize, numShards)
            );
        }

        @Override
        public InternalAggregations get(int index) {
            return aggsList.get(index);
        }

        @Override
        public int size() {
            return aggsList.size();
        }
    }

    @Param({ "32", "512" })
    private int bufferSize;

    @Benchmark
    public SearchPhaseController.ReducedQueryPhase reduceAggs(TermsList candidateList) throws Exception {
        List<QuerySearchResult> shards = new ArrayList<>();
        for (int i = 0; i < candidateList.size(); i++) {
            QuerySearchResult result = new QuerySearchResult();
            result.setShardIndex(i);
            result.from(0);
            result.size(0);
            result.topDocs(
                new TopDocsAndMaxScore(
                    new TopDocs(new TotalHits(1000, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), new ScoreDoc[0]),
                    Float.NaN
                ),
                new DocValueFormat[] { DocValueFormat.RAW }
            );
            result.aggregations(candidateList.get(i));
            result.setSearchShardTarget(
                new SearchShardTarget("node", new ShardId(new Index("index", "index"), i), null, OriginalIndices.NONE)
            );
            shards.add(result);
        }
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.terms("test")));
        request.setBatchedReduceSize(bufferSize);
        ExecutorService executor = Executors.newFixedThreadPool(1);
        QueryPhaseResultConsumer consumer = new QueryPhaseResultConsumer(
            request,
            executor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST),
            controller,
            SearchProgressListener.NOOP,
            namedWriteableRegistry,
            shards.size(),
            exc -> {},
            () -> false
        );
        CountDownLatch latch = new CountDownLatch(shards.size());
        for (int i = 0; i < shards.size(); i++) {
            consumer.consumeResult(shards.get(i), () -> latch.countDown());
        }
        latch.await();
        SearchPhaseController.ReducedQueryPhase phase = consumer.reduce();
        executor.shutdownNow();
        return phase;
    }
}
