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

package org.opensearch.search.aggregations.support;

import org.opensearch.search.aggregations.bucket.adjacency.InternalAdjacencyMatrix;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.search.aggregations.bucket.global.InternalGlobal;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.opensearch.search.aggregations.bucket.histogram.InternalVariableWidthHistogram;
import org.opensearch.search.aggregations.bucket.missing.InternalMissing;
import org.opensearch.search.aggregations.bucket.nested.InternalNested;
import org.opensearch.search.aggregations.bucket.nested.InternalReverseNested;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.sampler.InternalSampler;
import org.opensearch.search.aggregations.bucket.sampler.UnmappedSampler;
import org.opensearch.search.aggregations.bucket.terms.InternalSignificantTerms;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.UnmappedSignificantTerms;
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.aggregations.metrics.InternalExtendedStats;
import org.opensearch.search.aggregations.metrics.InternalGeoCentroid;
import org.opensearch.search.aggregations.metrics.InternalHDRPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMedianAbsoluteDeviation;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.InternalScriptedMetric;
import org.opensearch.search.aggregations.metrics.InternalStats;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentileRanks;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.InternalTopHits;
import org.opensearch.search.aggregations.metrics.InternalValueCount;
import org.opensearch.search.aggregations.metrics.InternalWeightedAvg;
import org.opensearch.search.aggregations.metrics.MetricInspectionHelper;
import org.opensearch.search.aggregations.pipeline.InternalBucketMetricValue;
import org.opensearch.search.aggregations.pipeline.InternalPercentilesBucket;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;

import java.util.stream.StreamSupport;

/**
 * Provides a set of static helpers to determine if a particular type of InternalAggregation "has a value"
 * or not.  This can be difficult to determine from an external perspective because each agg uses
 * different internal bookkeeping to determine if it is empty or not (NaN, +/-Inf, 0.0, etc).
 *
 * This set of helpers aim to ease that task by codifying what "empty" is for each agg.
 *
 * It is not entirely accurate for all aggs, since some do not expose or track the needed state
 * (e.g. sum doesn't record count, so it's not clear if the sum is 0.0 because it is empty
 * or because of summing to zero).  Pipeline aggs in particular are not well supported
 * by these helpers since most share InternalSimpleValue and it's not clear which pipeline
 * generated the value.
 *
 * @opensearch.internal
 */
public class AggregationInspectionHelper {
    public static <A extends InternalTerms<A, B>, B extends InternalTerms.Bucket<B>> boolean hasValue(InternalTerms<A, B> agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(UnmappedTerms agg) {
        return false;
    }

    public static boolean hasValue(UnmappedSignificantTerms agg) {
        return false;
    }

    public static boolean hasValue(UnmappedSampler agg) {
        return false;
    }

    public static boolean hasValue(InternalAdjacencyMatrix agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalFilters agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalFilter agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalGlobal agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalHistogram agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalDateHistogram agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalAutoDateHistogram agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalVariableWidthHistogram agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalComposite agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalMissing agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalNested agg) {
        return agg.getDocCount() > 0;
    }

    public static boolean hasValue(InternalReverseNested agg) {
        return agg.getDocCount() > 0;
    }

    public static <B extends InternalRange.Bucket, R extends InternalRange<B, R>> boolean hasValue(InternalRange<B, R> agg) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalSampler agg) {
        return agg.getDocCount() > 0;
    }

    public static <A extends InternalSignificantTerms<A, B>, B extends InternalSignificantTerms.Bucket<B>> boolean hasValue(
        InternalSignificantTerms<A, B> agg
    ) {
        return agg.getBuckets().stream().anyMatch(bucket -> bucket.getDocCount() > 0);
    }

    public static boolean hasValue(InternalAvg agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalSum agg) {
        // TODO this could be incorrect... e.g. +1 + -1
        return agg.getValue() != 0.0;
    }

    public static boolean hasValue(InternalCardinality agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalExtendedStats agg) {
        return agg.getCount() > 0;
    }

    public static boolean hasValue(InternalGeoCentroid agg) {
        return agg.centroid() != null && agg.count() > 0;
    }

    public static boolean hasValue(InternalHDRPercentileRanks agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalHDRPercentiles agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalMax agg) {
        return agg.getValue() != Double.NEGATIVE_INFINITY;
    }

    public static boolean hasValue(InternalMedianAbsoluteDeviation agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalMin agg) {
        return agg.getValue() != Double.POSITIVE_INFINITY;
    }

    public static boolean hasValue(InternalScriptedMetric agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalStats agg) {
        return agg.getCount() > 0;
    }

    public static boolean hasValue(InternalTDigestPercentileRanks agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalTDigestPercentiles agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalTopHits agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalValueCount agg) {
        return agg.getValue() > 0;
    }

    public static boolean hasValue(InternalWeightedAvg agg) {
        return MetricInspectionHelper.hasValue(agg);
    }

    public static boolean hasValue(InternalSimpleValue agg) {
        // This is a coarse approximation, since some aggs use positive/negative infinity or NaN
        return (Double.isInfinite(agg.getValue()) || Double.isNaN(agg.getValue())) == false;
    }

    public static boolean hasValue(InternalBucketMetricValue agg) {
        return Double.isInfinite(agg.value()) == false;
    }

    public static boolean hasValue(InternalPercentilesBucket agg) {
        return StreamSupport.stream(agg.spliterator(), false).allMatch(p -> Double.isNaN(p.getValue())) == false;
    }

}
