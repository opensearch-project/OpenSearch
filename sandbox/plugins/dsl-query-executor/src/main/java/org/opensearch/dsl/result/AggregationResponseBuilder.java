/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
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
 *
 * <p>A granularity is identified by its GROUP BY field names in <b>nesting order</b> (outer
 * bucket first) — the same order the {@code AggregationTreeWalker} accumulated them in and the
 * same order the response walk re-accumulates while descending the request's aggregation tree.
 * Results are keyed by the walker-produced {@link AggregationMetadata} carried on each plan, so
 * the key round-trips losslessly: sibling trees over the same field <em>set</em> but different
 * nesting order (e.g. {@code brand→category} vs {@code category→brand}) produce distinct plans
 * AND distinct keys. Re-deriving the key from the plan's group bit set would yield schema order
 * instead, forcing an order-insensitive (sorted) key under which such siblings collide.
 */
public final class AggregationResponseBuilder {

    /** NUL cannot appear in field names, so joined keys cannot collide. */
    private static final String AGGREGATION_LEVEL_SEPARATOR = "\u0000";

    private final AggregationRegistry registry;
    private final Map<String, ExecutionResult> granularityMap;

    public AggregationResponseBuilder(AggregationRegistry registry, List<ExecutionResult> aggResults) {
        this.registry = registry;
        this.granularityMap = new HashMap<>();
        for (ExecutionResult result : aggResults) {
            AggregationMetadata metadata = result.getPlan().aggregationMetadata();
            if (metadata == null) {
                throw new IllegalArgumentException(
                    "AGGREGATION plan is missing its AggregationMetadata — plans consumed by the "
                        + "response builder must be created by SearchSourceConverter"
                );
            }
            String key = granularityKey(metadata.getGroupByFieldNames());
            ExecutionResult previous = granularityMap.putIfAbsent(key, result);
            if (previous != null) {
                // The walker produces exactly one plan per nesting-order granularity, so a
                // duplicate key means the walker invariant broke upstream. Fail loudly instead
                // of silently overwriting one plan's results with another's.
                throw new IllegalStateException(
                    "Duplicate aggregation granularity [" + String.join(",", metadata.getGroupByFieldNames()) + "]"
                );
            }
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
        Map<String, Object> parentKeyFilter
    ) throws ConversionException {

        List<InternalAggregation> result = new ArrayList<>();

        for (AggregationBuilder agg : aggs) {
            @SuppressWarnings("unchecked")
            AggregationTranslator<AggregationBuilder> type = (AggregationTranslator<AggregationBuilder>) registry.get(agg.getClass());

            if (type instanceof MetricTranslator) {
                result.add(buildMetric((MetricTranslator<AggregationBuilder>) type, agg, accumulatedGroupFields, parentKeyFilter));
            } else if (type instanceof BucketTranslator) {
                result.add(buildBucket((BucketTranslator<AggregationBuilder>) type, agg, accumulatedGroupFields, parentKeyFilter));
            } else {
                throw new ConversionException(
                    "No response translator for aggregation [" + agg.getName() + "] of type " + agg.getClass().getSimpleName()
                );
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
        Map<String, Object> parentKeyFilter
    ) throws ConversionException {

        ExecutionResult result = granularityMap.get(granularityKey(accumulatedGroupFields));

        if (result == null) {
            return buildEmptyMetric(translator, agg);
        }

        List<Object[]> rows = StreamSupport.stream(result.getRows().spliterator(), false).collect(Collectors.toList());

        if (rows.isEmpty()) {
            return buildEmptyMetric(translator, agg);
        }

        Map<String, Integer> colIndex = buildColumnIndex(result);
        Integer colIdx = colIndex.get(agg.getName());

        if (colIdx == null) {
            throw new ConversionException("Metric column '" + agg.getName() + "' not found in aggregation result columns");
        }

        Object[] matchingRow = findMatchingRow(rows, colIndex, parentKeyFilter);
        Object value = (matchingRow != null) ? matchingRow[colIdx] : null;
        return translator.toInternalAggregation(agg.getName(), value, AggregationTranslator.userMetadata(agg));
    }

    /**
     * Builds an empty metric aggregation with no computed value.
     */
    private static InternalAggregation buildEmptyMetric(MetricTranslator<AggregationBuilder> translator, AggregationBuilder agg) {
        return translator.toInternalAggregation(agg.getName(), null, AggregationTranslator.userMetadata(agg));
    }

    /**
     * Builds an empty bucket aggregation with no buckets.
     */
    private static InternalAggregation buildEmptyBucket(BucketTranslator<AggregationBuilder> translator, AggregationBuilder agg) {
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
        Map<String, Object> parentKeyFilter
    ) throws ConversionException {

        GroupingInfo grouping = translator.getGrouping(agg);
        List<String> newGroupFields = new ArrayList<>(accumulatedGroupFields);
        newGroupFields.addAll(grouping.getFieldNames());

        ExecutionResult result = granularityMap.get(granularityKey(newGroupFields));

        if (result == null) {
            return buildEmptyBucket(translator, agg);
        }

        List<Object[]> rows = StreamSupport.stream(result.getRows().spliterator(), false).collect(Collectors.toList());

        if (rows.isEmpty()) {
            return buildEmptyBucket(translator, agg);
        }

        Map<String, Integer> colIndex = buildColumnIndex(result);
        List<Object[]> filteredRows = filterRows(rows, colIndex, parentKeyFilter);

        List<String> currentGroupColumns = new ArrayList<>(grouping.getFieldNames());

        Map<List<Object>, List<Object[]>> grouped = groupRowsByKeys(filteredRows, currentGroupColumns, colIndex);

        Integer countIdx = colIndex.get(AggregationMetadataBuilder.IMPLICIT_COUNT_NAME);
        if (countIdx == null) {
            throw new ConversionException("Missing " + AggregationMetadataBuilder.IMPLICIT_COUNT_NAME + " column in aggregation result");
        }

        List<BucketEntry> buckets = new ArrayList<>();
        List<AggregationBuilder> subAggs = new ArrayList<>(translator.getSubAggregations(agg));

        for (Map.Entry<List<Object>, List<Object[]>> entry : grouped.entrySet()) {
            Map<String, Object> childFilter = new HashMap<>(parentKeyFilter);
            for (int i = 0; i < currentGroupColumns.size(); i++) {
                childFilter.put(currentGroupColumns.get(i), entry.getKey().get(i));
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
    private static Object[] findMatchingRow(List<Object[]> rows, Map<String, Integer> colIndex, Map<String, Object> filter) {
        for (Object[] row : rows) {
            if (matchesFilter(row, colIndex, filter)) {
                return row;
            }
        }
        return null;
    }

    // TODO: Avoid re-scanning the full row list on every recursion (here and in findMatchingRow).
    // Index each granularity's rows by parent bucket key once, then look buckets up directly.
    /**
     * Filters rows to only those matching all filter criteria.
     * Ensures nested aggregations only process rows belonging to their parent bucket.
     */
    private static List<Object[]> filterRows(List<Object[]> rows, Map<String, Integer> colIndex, Map<String, Object> filter) {
        if (filter.isEmpty()) {
            return rows;
        }
        return rows.stream().filter(row -> matchesFilter(row, colIndex, filter)).collect(Collectors.toList());
    }

    /**
     * Checks if a row matches all filter criteria.
     * Uses type-coercion comparison to handle numeric type differences from execution engine.
     */
    private static boolean matchesFilter(Object[] row, Map<String, Integer> colIndex, Map<String, Object> filter) {
        for (Map.Entry<String, Object> entry : filter.entrySet()) {
            Integer idx = colIndex.get(entry.getKey());
            if (idx == null || !ComparisonUtils.valuesEqual(row[idx], entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Groups rows by their grouping column values (the bucket keys). Group columns are
     * resolved by name — they are not guaranteed to be the leading row positions.
     */
    private static Map<List<Object>, List<Object[]>> groupRowsByKeys(
        List<Object[]> rows,
        List<String> groupColumns,
        Map<String, Integer> colIndex
    ) throws ConversionException {
        List<Integer> keyIndices = new ArrayList<>(groupColumns.size());
        for (String column : groupColumns) {
            Integer idx = colIndex.get(column);
            if (idx == null) {
                throw new ConversionException("Group column '" + column + "' not found in aggregation result columns");
            }
            keyIndices.add(idx);
        }

        Map<List<Object>, List<Object[]>> grouped = new LinkedHashMap<>();
        for (Object[] row : rows) {
            List<Object> key = new ArrayList<>(keyIndices.size());
            for (int idx : keyIndices) {
                key.add(row[idx]);
            }
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
        return grouped;
    }

    /**
     * Canonical granularity key: group field names in nesting order (outer bucket first),
     * NUL-joined. Insertion uses the walker's {@link AggregationMetadata#getGroupByFieldNames()}
     * and lookup uses the response walk's accumulated fields — both are built in nesting order,
     * so the key matches by construction. Not sorted: sorting would erase nesting order, the
     * one thing distinguishing sibling trees over the same field set (see class javadoc).
     */
    private static String granularityKey(List<String> groupFieldNames) {
        return String.join(AGGREGATION_LEVEL_SEPARATOR, groupFieldNames);
    }
}
