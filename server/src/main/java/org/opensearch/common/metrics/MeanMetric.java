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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.metrics;

import org.opensearch.common.annotation.PublicApi;

import java.util.concurrent.atomic.LongAdder;

/**
 * An average metric for tracking.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class MeanMetric implements Metric {

    private final LongAdder counter = new LongAdder();
    private final LongAdder sum = new LongAdder();

    public void inc(long n) {
        counter.increment();
        sum.add(n);
    }

    public void add(MeanMetric other) {
        counter.add(other.counter.sum());
        sum.add(other.sum.sum());
    }

    public void dec(long n) {
        counter.decrement();
        sum.add(-n);
    }

    public long count() {
        return counter.sum();
    }

    public long sum() {
        return sum.sum();
    }

    public double mean() {
        long count = count();
        if (count > 0) {
            return sum.sum() / (double) count;
        }
        return 0.0;
    }

    public void clear() {
        counter.reset();
        sum.reset();
    }

}
