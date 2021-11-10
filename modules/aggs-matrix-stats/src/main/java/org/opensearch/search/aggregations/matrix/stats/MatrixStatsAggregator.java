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

package org.opensearch.search.aggregations.matrix.stats;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.index.fielddata.NumericDoubleValues;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.aggregations.support.ArrayValuesSource;

import java.io.IOException;
import java.util.Map;

/**
 * Metric Aggregation for computing the pearson product correlation coefficient between multiple fields
 **/
final class MatrixStatsAggregator extends MetricsAggregator {
    /** Multiple ValuesSource with field names */
    private final ArrayValuesSource.NumericArrayValuesSource valuesSources;

    /** array of descriptive stats, per shard, needed to compute the correlation */
    ObjectArray<RunningStats> stats;

    MatrixStatsAggregator(
        String name,
        Map<String, ValuesSource.Numeric> valuesSources,
        SearchContext context,
        Aggregator parent,
        MultiValueMode multiValueMode,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        if (valuesSources != null && !valuesSources.isEmpty()) {
            this.valuesSources = new ArrayValuesSource.NumericArrayValuesSource(valuesSources, multiValueMode);
            stats = context.bigArrays().newObjectArray(1);
        } else {
            this.valuesSources = null;
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return (valuesSources != null && valuesSources.needsScores()) ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final NumericDoubleValues[] values = new NumericDoubleValues[valuesSources.fieldNames().length];
        for (int i = 0; i < values.length; ++i) {
            values[i] = valuesSources.getField(i, ctx);
        }

        return new LeafBucketCollectorBase(sub, values) {
            final String[] fieldNames = valuesSources.fieldNames();
            final double[] fieldVals = new double[fieldNames.length];

            @Override
            public void collect(int doc, long bucket) throws IOException {
                // get fields
                if (includeDocument(doc)) {
                    stats = bigArrays.grow(stats, bucket + 1);
                    RunningStats stat = stats.get(bucket);
                    // add document fields to correlation stats
                    if (stat == null) {
                        stat = new RunningStats(fieldNames, fieldVals);
                        stats.set(bucket, stat);
                    } else {
                        stat.add(fieldNames, fieldVals);
                    }
                }
            }

            /**
             * return a map of field names and data
             */
            private boolean includeDocument(int doc) throws IOException {
                // loop over fields
                for (int i = 0; i < fieldVals.length; ++i) {
                    final NumericDoubleValues doubleValues = values[i];
                    if (doubleValues.advanceExact(doc)) {
                        final double value = doubleValues.doubleValue();
                        if (value == Double.NEGATIVE_INFINITY) {
                            // TODO: Fix matrix stats to treat neg inf as any other value
                            return false;
                        }
                        fieldVals[i] = value;
                    } else {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSources == null || bucket >= stats.size()) {
            return buildEmptyAggregation();
        }
        return new InternalMatrixStats(name, stats.size(), stats.get(bucket), null, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMatrixStats(name, 0, null, null, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(stats);
    }
}
