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

package org.opensearch.search.aggregations;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntConsumer;

/**
 * An aggregation service that creates instances of {@link MultiBucketConsumer}.
 * The consumer is used by {@link BucketsAggregator} and {@link InternalMultiBucketAggregation} to limit the number of buckets created
 * in {@link Aggregator#buildAggregations} and {@link InternalAggregation#reduce}.
 * The limit can be set by changing the `search.max_buckets` cluster setting and defaults to 65535.
 *
 * @opensearch.internal
 */
public class MultiBucketConsumerService {
    public static final int DEFAULT_MAX_BUCKETS = 65535;
    public static final Setting<Integer> MAX_BUCKET_SETTING = Setting.intSetting(
        "search.max_buckets",
        DEFAULT_MAX_BUCKETS,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final CircuitBreaker breaker;

    private volatile int maxBucket;

    public MultiBucketConsumerService(ClusterService clusterService, Settings settings, CircuitBreaker breaker) {
        this.breaker = breaker;
        this.maxBucket = MAX_BUCKET_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BUCKET_SETTING, this::setMaxBucket);
    }

    private void setMaxBucket(int maxBucket) {
        this.maxBucket = maxBucket;
    }

    /**
     * Thrown when there are too many buckets
     *
     * @opensearch.internal
     */
    public static class TooManyBucketsException extends AggregationExecutionException {
        private final int maxBuckets;

        public TooManyBucketsException(String message, int maxBuckets) {
            super(message);
            this.maxBuckets = maxBuckets;
        }

        public TooManyBucketsException(StreamInput in) throws IOException {
            super(in);
            maxBuckets = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(maxBuckets);
        }

        public int getMaxBuckets() {
            return maxBuckets;
        }

        @Override
        public RestStatus status() {
            return RestStatus.SERVICE_UNAVAILABLE;
        }

        @Override
        protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("max_buckets", maxBuckets);
        }
    }

    /**
     * An {@link IntConsumer} that throws a {@link TooManyBucketsException}
     * when the sum of the provided values is above the limit (`search.max_buckets`).
     * It is used by aggregators to limit the number of bucket creation during
     * {@link Aggregator#buildAggregations} and {@link InternalAggregation#reduce}.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class MultiBucketConsumer implements IntConsumer {
        private final int limit;
        private final CircuitBreaker breaker;

        // count is currently only updated in final reduce phase which is executed in single thread for both concurrent and non-concurrent
        // search
        private int count;
        // will be updated by multiple threads in concurrent search hence making it as LongAdder
        private final LongAdder callCount;
        private volatile boolean circuitBreakerTripped;
        private final int availProcessors;

        public MultiBucketConsumer(int limit, CircuitBreaker breaker) {
            this.limit = limit;
            this.breaker = breaker;
            callCount = new LongAdder();
            availProcessors = Runtime.getRuntime().availableProcessors();
        }

        // only visible for testing
        protected MultiBucketConsumer(
            int limit,
            CircuitBreaker breaker,
            LongAdder callCount,
            boolean circuitBreakerTripped,
            int availProcessors
        ) {
            this.limit = limit;
            this.breaker = breaker;
            this.callCount = callCount;
            this.circuitBreakerTripped = circuitBreakerTripped;
            this.availProcessors = availProcessors;
        }

        @Override
        public void accept(int value) {
            if (value != 0) {
                count += value;
                if (count > limit) {
                    throw new TooManyBucketsException(
                        "Trying to create too many buckets. Must be less than or equal to: ["
                            + limit
                            + "] but was ["
                            + count
                            + "]. This limit can be set by changing the ["
                            + MAX_BUCKET_SETTING.getKey()
                            + "] cluster level setting.",
                        limit
                    );
                }
            }
            callCount.increment();
            // tripping the circuit breaker for other threads in case of concurrent search
            // if the circuit breaker has tripped for one of the threads already, more info
            // can be found on: https://github.com/opensearch-project/OpenSearch/issues/7785
            if (circuitBreakerTripped) {
                throw new CircuitBreakingException(
                    "Circuit breaker for this consumer has already been tripped by previous invocations. "
                        + "This can happen in case of concurrent segment search when multiple threads are "
                        + "executing the request and one of the thread has already tripped the circuit breaker",
                    breaker.getDurability()
                );
            }
            // check parent circuit breaker every 1024 to (1024 + available processors) calls
            long sum = callCount.sum();
            if ((sum >= 1024) && (sum & 0x3FF) <= availProcessors) {
                try {
                    breaker.addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
                } catch (CircuitBreakingException e) {
                    circuitBreakerTripped = true;
                    throw e;
                }
            }
        }

        public void reset() {
            this.count = 0;
        }

        public int getCount() {
            return count;
        }

        public int getLimit() {
            return limit;
        }
    }

    public MultiBucketConsumer create() {
        return new MultiBucketConsumer(maxBucket, breaker);
    }
}
