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

import org.HdrHistogram.DoubleHistogram;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of HDR percentiles ranks
 *
 * @opensearch.internal
 */
public class InternalHDRPercentileRanks extends AbstractInternalHDRPercentiles implements PercentileRanks {
    public static final String NAME = "hdr_percentile_ranks";

    public InternalHDRPercentileRanks(
        String name,
        double[] cdfValues,
        DoubleHistogram state,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, cdfValues, state, keyed, formatter, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalHDRPercentileRanks(StreamInput in) throws IOException {
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
    public double percent(double value) {
        return percentileRank(state, value);
    }

    @Override
    public String percentAsString(double value) {
        return valueAsString(String.valueOf(value));
    }

    @Override
    public double value(double key) {
        return percent(key);
    }

    @Override
    protected AbstractInternalHDRPercentiles createReduced(
        String name,
        double[] keys,
        DoubleHistogram merged,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        return new InternalHDRPercentileRanks(name, keys, merged, keyed, format, metadata);
    }

    public static double percentileRank(DoubleHistogram state, double value) {
        if (state.getTotalCount() == 0) {
            return Double.NaN;
        }
        double percentileRank = state.getPercentileAtOrBelowValue(value);
        if (percentileRank < 0) {
            percentileRank = 0;
        } else if (percentileRank > 100) {
            percentileRank = 100;
        }
        return percentileRank;
    }

    /**
     * Terator for HDR percentile ranks agg
     *
     * @opensearch.internal
     */
    public static class Iter implements Iterator<Percentile> {

        private final double[] values;
        private final DoubleHistogram state;
        private int i;

        public Iter(double[] values, DoubleHistogram state) {
            this.values = values;
            this.state = state;
            i = 0;
        }

        @Override
        public boolean hasNext() {
            return i < values.length;
        }

        @Override
        public Percentile next() {
            final Percentile next = new Percentile(percentileRank(state, values[i]), values[i]);
            ++i;
            return next;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
