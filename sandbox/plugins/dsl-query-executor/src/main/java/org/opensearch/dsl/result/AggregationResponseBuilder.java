/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationType;
import org.opensearch.dsl.aggregation.ExpressionGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.aggregation.bucket.BucketTranslator;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.util.ComparisonUtils;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Converts execution results into OpenSearch InternalAggregations format.
 * Simplified version for the new architecture using Iterable of Object arrays.
 */
public final class AggregationResponseBuilder {

    private final AggregationRegistry registry;
    private final Map<String, ExecutionResult> granularityMap;

    public AggregationResponseBuilder(AggregationRegistry registry, List<ExecutionResult> aggResults) {
        this.registry = registry;
        this.granularityMap = new HashMap<>();
        for (ExecutionResult result : aggResults) {
            String key = granularityKey(result);
            granularityMap.put(key, result);
        }
    }

    public InternalAggregations build(Collection<AggregationBuilder> originalAggs) throws ConversionException {
        List<InternalAggregation> aggs = buildLevel(originalAggs, new ArrayList<>(), Map.of());
        return InternalAggregations.from(aggs);
    }

    @SuppressWarnings("unchecked")
    private List<InternalAggregation> buildLevel(
            Collection<AggregationBuilder> aggs,
            List<String> accumulatedGroupFields,
            Map<String, Object> parentKeyFilter) throws ConversionException {

        List<InternalAggregation> result = new ArrayList<>();

        for (AggregationBuilder agg : aggs) {
            @SuppressWarnings("unchecked")
            AggregationType<AggregationBuilder> type = (AggregationType<AggregationBuilder>) registry.get(agg.getClass());

            if (type instanceof MetricTranslator) {
                result.add(buildMetric((MetricTranslator<AggregationBuilder>) type, agg,
                    accumulatedGroupFields, parentKeyFilter));
            } else if (type instanceof BucketTranslator) {
                result.add(buildBucket((BucketTranslator<AggregationBuilder>) type, agg,
                    accumulatedGroupFields, parentKeyFilter));
            }
        }
        return result;
    }

    private InternalAggregation buildMetric(
            MetricTranslator<AggregationBuilder> translator,
            AggregationBuilder agg,
            List<String> accumulatedGroupFields,
            Map<String, Object> parentKeyFilter) {

        String granularityKey = String.join(",", accumulatedGroupFields);
        ExecutionResult result = granularityMap.get(granularityKey);

        if (result == null) {
            return translator.toInternalAggregation(agg.getName(), null);
        }

        List<Object[]> rows = StreamSupport.stream(result.getRows().spliterator(), false)
            .collect(Collectors.toList());

        if (rows.isEmpty()) {
            return translator.toInternalAggregation(agg.getName(), null);
        }

        Map<String, Integer> colIndex = buildColumnIndex(result);
        Integer colIdx = colIndex.get(agg.getName());

        if (colIdx == null) {
            return translator.toInternalAggregation(agg.getName(), null);
        }

        if (accumulatedGroupFields.isEmpty()) {
            Object value = rows.get(0)[colIdx];
            return translator.toInternalAggregation(agg.getName(), value);
        }

        Object[] matchingRow = findMatchingRow(rows, colIndex, parentKeyFilter);
        Object value = matchingRow != null ? matchingRow[colIdx] : null;
        return translator.toInternalAggregation(agg.getName(), value);
    }

    private InternalAggregation buildBucket(
            BucketTranslator<AggregationBuilder> translator,
            AggregationBuilder agg,
            List<String> accumulatedGroupFields,
            Map<String, Object> parentKeyFilter) throws ConversionException {

        GroupingInfo grouping = translator.getGrouping(agg);
        List<String> newGroupFields = new ArrayList<>(accumulatedGroupFields);

        if (grouping instanceof ExpressionGrouping) {
            newGroupFields.add(((ExpressionGrouping) grouping).getProjectedColumnName());
        } else {
            newGroupFields.addAll(grouping.getFieldNames());
        }

        String granularityKey = String.join(",", newGroupFields);
        ExecutionResult result = granularityMap.get(granularityKey);

        if (result == null) {
            return translator.toBucketAggregation(agg, List.of());
        }

        List<Object[]> rows = StreamSupport.stream(result.getRows().spliterator(), false)
            .collect(Collectors.toList());

        if (rows.isEmpty()) {
            return translator.toBucketAggregation(agg, List.of());
        }

        Map<String, Integer> colIndex = buildColumnIndex(result);
        List<Object[]> filteredRows = filterRows(rows, colIndex, parentKeyFilter);

        List<String> currentGroupColumns = new ArrayList<>();
        if (grouping instanceof ExpressionGrouping expr) {
            currentGroupColumns.add(expr.getProjectedColumnName());
        } else {
            currentGroupColumns.addAll(grouping.getFieldNames());
        }

        Map<List<Object>, List<Object[]>> grouped = groupByKeys(filteredRows, currentGroupColumns.size(), colIndex);

        List<BucketEntry> buckets = new ArrayList<>();
        Collection<AggregationBuilder> subAggs = translator.getSubAggregations(agg);

        for (Map.Entry<List<Object>, List<Object[]>> entry : grouped.entrySet()) {
            Map<String, Object> childFilter = new HashMap<>(parentKeyFilter);
            for (int i = 0; i < currentGroupColumns.size(); i++) {
                childFilter.put(currentGroupColumns.get(i), entry.getKey().get(i));
            }

            long docCount = ((Number) entry.getValue().get(0)[colIndex.get("_count")]).longValue();
            InternalAggregations subAggregations = subAggs.isEmpty()
                ? InternalAggregations.EMPTY
                : InternalAggregations.from(buildLevel(subAggs, newGroupFields, childFilter));

            buckets.add(new BucketEntry(entry.getKey(), docCount, subAggregations));
        }

        return translator.toBucketAggregation(agg, buckets);
    }

    private static Map<String, Integer> buildColumnIndex(ExecutionResult result) {
        Map<String, Integer> index = new HashMap<>();
        List<String> fieldNames = result.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            index.put(fieldNames.get(i), i);
        }
        return index;
    }

    private static Object[] findMatchingRow(List<Object[]> rows, Map<String, Integer> colIndex,
            Map<String, Object> filter) {
        for (Object[] row : rows) {
            if (rowMatchesFilter(row, colIndex, filter)) {
                return row;
            }
        }
        return null;
    }

    private static List<Object[]> filterRows(List<Object[]> rows, Map<String, Integer> colIndex,
            Map<String, Object> filter) {
        if (filter.isEmpty()) {
            return rows;
        }
        return rows.stream()
            .filter(row -> rowMatchesFilter(row, colIndex, filter))
            .collect(Collectors.toList());
    }

    private static boolean rowMatchesFilter(Object[] row, Map<String, Integer> colIndex,
            Map<String, Object> filter) {
        for (Map.Entry<String, Object> entry : filter.entrySet()) {
            Integer idx = colIndex.get(entry.getKey());
            if (idx == null || !ComparisonUtils.valuesEqual(row[idx], entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private static Map<List<Object>, List<Object[]>> groupByKeys(
            List<Object[]> rows, int keyCount, Map<String, Integer> colIndex) {
        Map<List<Object>, List<Object[]>> grouped = new HashMap<>();

        for (Object[] row : rows) {
            List<Object> key = new ArrayList<>(keyCount);
            for (int i = 0; i < keyCount; i++) {
                key.add(row[i]);
            }
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        return grouped;
    }

    private static String granularityKey(ExecutionResult result) {
        RelNode relNode = result.getPlan().relNode();
        if (relNode instanceof LogicalAggregate agg) {
            int groupCount = agg.getGroupCount();
            return result.getFieldNames().stream()
                .limit(groupCount)
                .collect(Collectors.joining(","));
        }
        return "";
    }
}
