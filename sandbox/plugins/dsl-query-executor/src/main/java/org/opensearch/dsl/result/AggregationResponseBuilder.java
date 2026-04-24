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
import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.aggregation.bucket.BucketTranslator;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.util.ComparisonUtils;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Converts execution results into OpenSearch InternalAggregations format.
 * Uses granularity-based matching to map flat tabular results to hierarchical aggregation structures.
 */
public final class AggregationResponseBuilder {

    private static final String NO_GROUPING_KEY = "";
    private static final String AGGREGATION_LEVEL_SEPARATOR = ",";

    private final AggregationRegistry registry;
    private final Map<String, ExecutionResult> granularityMap;

    public AggregationResponseBuilder(AggregationRegistry registry, List<ExecutionResult> aggResults) {
        this.registry = registry;
        this.granularityMap = new HashMap<>();
        for (ExecutionResult result : aggResults) {
            String key = computeGranularityKey(result);
            granularityMap.put(key, result);
        }
    }

    /**
     * Builds InternalAggregations from the original aggregation builders.
     */
    public InternalAggregations build(List<AggregationBuilder> originalAggs) throws ConversionException {
        List<InternalAggregation> aggs = buildLevel(originalAggs, new ArrayList<>(), Map.of());
        return InternalAggregations.from(aggs);
    }

    /**
     * Recursively builds aggregations at a specific nesting level.
     * Routes to buildMetric or buildBucket based on aggregation type.
     */
    private List<InternalAggregation> buildLevel(
            List<AggregationBuilder> aggs,
            List<String> accumulatedGroupFields,
            Map<String, Object> parentKeyFilter) throws ConversionException {

        List<InternalAggregation> result = new ArrayList<>();

        for (AggregationBuilder agg : aggs) {
            @SuppressWarnings("unchecked")
            AggregationTranslator<AggregationBuilder> type = (AggregationTranslator<AggregationBuilder>) registry.get(agg.getClass());

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

    /**
     * Builds a metric aggregation by extracting the computed value from execution results.
     * Finds the matching row using granularity key and parent filters.
     */
    private InternalAggregation buildMetric(
            MetricTranslator<AggregationBuilder> translator,
            AggregationBuilder agg,
            List<String> accumulatedGroupFields,
            Map<String, Object> parentKeyFilter) {

        String granularityKey = String.join(AGGREGATION_LEVEL_SEPARATOR, accumulatedGroupFields);
        ExecutionResult result = granularityMap.get(granularityKey);

        if (result == null) {
            return buildEmptyMetric(translator, agg);
        }

        List<Object[]> rows = StreamSupport.stream(result.getRows().spliterator(), false)
            .collect(Collectors.toList());

        if (rows.isEmpty()) {
            return buildEmptyMetric(translator, agg);
        }

        Map<String, Integer> colIndex = buildColumnIndex(result);
        Integer colIdx = colIndex.get(agg.getName());

        if (colIdx == null) {
            return buildEmptyMetric(translator, agg);
        }

        Object[] matchingRow = findMatchingRow(rows, colIndex, parentKeyFilter);
        Object value = (matchingRow != null) ? matchingRow[colIdx] : null;
        return translator.toInternalAggregation(agg.getName(), value);
    }

    /**
     * Builds an empty metric aggregation with no computed value.
     */
    private static InternalAggregation buildEmptyMetric(
            MetricTranslator<AggregationBuilder> translator,
            AggregationBuilder agg) {
        return translator.toInternalAggregation(agg.getName(), null);
    }

    /**
     * Builds an empty bucket aggregation with no buckets.
     */
    private static InternalAggregation buildEmptyBucket(
            BucketTranslator<AggregationBuilder> translator,
            AggregationBuilder agg) {
        return translator.toBucketAggregation(agg, List.of());
    }

    /**
     * Builds a bucket aggregation by grouping rows and recursively building sub-aggregations.
     * Groups rows by bucket keys and recursively processes nested aggregations for each bucket.
     */
    private InternalAggregation buildBucket(
            BucketTranslator<AggregationBuilder> translator,
            AggregationBuilder agg,
            List<String> accumulatedGroupFields,
            Map<String, Object> parentKeyFilter) throws ConversionException {

        GroupingInfo grouping = translator.getGrouping(agg);
        List<String> newGroupFields = new ArrayList<>(accumulatedGroupFields);
        newGroupFields.addAll(grouping.getFieldNames());

        String granularityKey = String.join(AGGREGATION_LEVEL_SEPARATOR, newGroupFields);
        ExecutionResult result = granularityMap.get(granularityKey);

        if (result == null) {
            return buildEmptyBucket(translator, agg);
        }

        List<Object[]> rows = StreamSupport.stream(result.getRows().spliterator(), false)
            .collect(Collectors.toList());

        if (rows.isEmpty()) {
            return buildEmptyBucket(translator, agg);
        }

        Map<String, Integer> colIndex = buildColumnIndex(result);
        List<Object[]> filteredRows = filterRows(rows, colIndex, parentKeyFilter);

        List<String> currentGroupColumns = new ArrayList<>(grouping.getFieldNames());

        Map<List<Object>, List<Object[]>> grouped = groupRowsByKeys(filteredRows, currentGroupColumns.size(), colIndex);

        List<BucketEntry> buckets = new ArrayList<>();
        List<AggregationBuilder> subAggs = new ArrayList<>(translator.getSubAggregations(agg));

        for (Map.Entry<List<Object>, List<Object[]>> entry : grouped.entrySet()) {
            Map<String, Object> childFilter = new HashMap<>(parentKeyFilter);
            for (int i = 0; i < currentGroupColumns.size(); i++) {
                childFilter.put(currentGroupColumns.get(i), entry.getKey().get(i));
            }

            Integer countIdx = colIndex.get("_count");
            if (countIdx == null) {
                throw new ConversionException("Missing _count column in aggregation result");
            }
            Object[] firstRowInGroup = entry.getValue().get(0);
            long docCount = ((Number) firstRowInGroup[countIdx]).longValue();

            InternalAggregations subAggregations = subAggs.isEmpty()
                ? InternalAggregations.EMPTY
                : InternalAggregations.from(buildLevel(subAggs, newGroupFields, childFilter));

            buckets.add(new BucketEntry(entry.getKey(), docCount, subAggregations));
        }

        return translator.toBucketAggregation(agg, buckets);
    }

    /**
     * Builds a map from column names to their indices.
     * Enables efficient column lookup by name during row processing.
     */
    private static Map<String, Integer> buildColumnIndex(ExecutionResult result) {
        Map<String, Integer> index = new HashMap<>();
        List<String> fieldNames = result.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            index.put(fieldNames.get(i), i);
        }
        return index;
    }

    /**
     * Finds the first row matching all filter criteria.
     * Used to locate the specific row for nested metric aggregations.
     */
    private static Object[] findMatchingRow(List<Object[]> rows, Map<String, Integer> colIndex,
            Map<String, Object> filter) {
        for (Object[] row : rows) {
            if (matchesFilter(row, colIndex, filter)) {
                return row;
            }
        }
        return null;
    }

    /**
     * Filters rows to only those matching all filter criteria.
     * Ensures nested aggregations only process rows belonging to their parent bucket.
     */
    private static List<Object[]> filterRows(List<Object[]> rows, Map<String, Integer> colIndex,
            Map<String, Object> filter) {
        if (filter.isEmpty()) {
            return rows;
        }
        return rows.stream()
            .filter(row -> matchesFilter(row, colIndex, filter))
            .collect(Collectors.toList());
    }

    /**
     * Checks if a row matches all filter criteria.
     * Uses type-coercion comparison to handle numeric type differences from execution engine.
     */
    private static boolean matchesFilter(Object[] row, Map<String, Integer> colIndex,
            Map<String, Object> filter) {
        for (Map.Entry<String, Object> entry : filter.entrySet()) {
            Integer idx = colIndex.get(entry.getKey());
            if (idx == null || !ComparisonUtils.valuesEqual(row[idx], entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Groups rows by their grouping column values (the bucket keys).
     * Creates buckets where each unique combination of grouping values becomes a separate bucket.
     */
    private static Map<List<Object>, List<Object[]>> groupRowsByKeys(
            List<Object[]> rows, int groupColumnCount, Map<String, Integer> colIndex) {
        Map<List<Object>, List<Object[]>> grouped = new LinkedHashMap<>();

        for (Object[] row : rows) {
            List<Object> key = new ArrayList<>(groupColumnCount);
            for (int i = 0; i < groupColumnCount; i++) {
                key.add(row[i]);
            }
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        return grouped;
    }

    /**
     * Computes the aggregation level key (comma-separated group field names) for an execution result.
     * Used to match execution results with the appropriate aggregation nesting level.
     */
    private static String computeGranularityKey(ExecutionResult result) {
        RelNode relNode = result.getPlan().relNode();
        if (relNode instanceof LogicalAggregate agg) {
            int groupCount = agg.getGroupCount();
            return result.getFieldNames().stream()
                .limit(groupCount)
                .collect(Collectors.joining(AGGREGATION_LEVEL_SEPARATOR));
        }
        return NO_GROUPING_KEY;
    }
}
