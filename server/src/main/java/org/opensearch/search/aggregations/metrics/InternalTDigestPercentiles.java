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

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of TDigest percentiles agg
 *
 * @opensearch.internal
 */
public class InternalTDigestPercentiles extends AbstractInternalTDigestPercentiles implements Percentiles {
    public static final String NAME = "tdigest_percentiles";

    public InternalTDigestPercentiles(
        String name,
        double[] percents,
        TDigestState state,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, percents, state, keyed, formatter, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalTDigestPercentiles(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Iterator<Percentile> iterator() {
        return new Iter(keys, state);
    }

    @Override
    public double percentile(double percent) {
        return state.quantile(percent / 100);
    }

    @Override
    public String percentileAsString(double percent) {
        return valueAsString(String.valueOf(percent));
    }

    @Override
    public double value(double key) {
        return percentile(key);
    }

    @Override
    protected AbstractInternalTDigestPercentiles createReduced(
        String name,
        double[] keys,
        TDigestState merged,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        return new InternalTDigestPercentiles(name, keys, merged, keyed, format, metadata);
    }

    /**
     * Iter for the TDigest percentiles agg
     *
     * @opensearch.internal
     */
    public static class Iter implements Iterator<Percentile> {

        private final double[] percents;
        private final TDigestState state;
        private int i;

        public Iter(double[] percents, TDigestState state) {
            this.percents = percents;
            this.state = state;
            i = 0;
        }

        @Override
        public boolean hasNext() {
            return i < percents.length;
        }

        @Override
        public Percentile next() {
            final Percentile next = new Percentile(percents[i], state.quantile(percents[i] / 100));
            ++i;
            return next;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
