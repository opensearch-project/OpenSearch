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
import org.opensearch.dsl.aggregation.bucket.SizedBucketTranslator;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.converter.PostAggregateConverter;
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
 *
 * <p>Plans are per bucket aggregation, so results are keyed by the <b>aggregation-name path</b>
 * (outer bucket first) — the walker records it on each plan's {@link AggregationMetadata}, and
 * the response walk re-accumulates the same path while descending the request tree, so the key
 * matches by construction. Aggregation names are unique among siblings by DSL contract, which
 * makes the key unambiguous: sibling aggregations over the same field, or sibling trees over
 * the same field set in different nesting orders, all have distinct paths and distinct plans.
 */
public final class AggregationResponseBuilder {

    private final AggregationRegistry registry;
    private final Map<String, ExecutionResult> resultsByAggPath;
    private final CountTotals countTotals;

    public AggregationResponseBuilder(AggregationRegistry registry, List<ExecutionResult> aggResults) {
        this(registry, aggResults, null);
    }

    public AggregationResponseBuilder(AggregationRegistry registry, List<ExecutionResult> aggResults, CountTotals countTotals) {
        this.registry = registry;
        this.countTotals = countTotals;
        this.resultsByAggPath = new HashMap<>();
        for (ExecutionResult result : aggResults) {
            AggregationMetadata metadata = result.getPlan().aggregationMetadata();
            if (metadata == null) {
                throw new IllegalArgumentException(
                    "AGGREGATION plan is missing its AggregationMetadata — plans consumed by the "
                        + "response builder must be created by SearchSourceConverter"
                );
            }
            String key = pathKey(metadata.getAggNamePath());
            ExecutionResult previous = resultsByAggPath.putIfAbsent(key, result);
            if (previous != null) {
                // Sibling aggregation names are unique by DSL contract, so a duplicate path
                // means the walker invariant broke upstream. Fail loudly instead of silently
                // overwriting one plan's results with another's.
                throw new IllegalStateException("Duplicate aggregation plan path [" + String.join(",", metadata.getAggNamePath()) + "]");
            }
        }
    }

    // TODO: Support pipeline aggregations. They post-process the assembled aggregation tree;
    // currently they are ignored.
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
        List<String> accumulatedAggNames,
        Map<String, Object> parentKeyFilter
    ) throws ConversionException {

        List<InternalAggregation> result = new ArrayList<>();

        for (AggregationBuilder agg : aggs) {
            @SuppressWarnings("unchecked")
            AggregationTranslator<AggregationBuilder> type = (AggregationTranslator<AggregationBuilder>) registry.get(agg.getClass());

            if (type instanceof MetricTranslator) {
                result.add(buildMetric((MetricTranslator<AggregationBuilder>) type, agg, accumulatedAggNames, parentKeyFilter));
            } else if (type instanceof BucketTranslator) {
                result.add(buildBucket((BucketTranslator<AggregationBuilder>) type, agg, accumulatedAggNames, parentKeyFilter));
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
     * Metrics ride in their enclosing bucket aggregation's plan, so the lookup uses the
     * enclosing aggregation-name path; parent filters locate the specific row.
     */
    private InternalAggregation buildMetric(
        MetricTranslator<AggregationBuilder> translator,
        AggregationBuilder agg,
        List<String> accumulatedAggNames,
        Map<String, Object> parentKeyFilter
    ) throws ConversionException {

        ExecutionResult result = resultsByAggPath.get(pathKey(accumulatedAggNames));

        if (result == null) {
            return buildEmptyMetric(translator, agg);
        }

        List<Object[]> rows = materialize(result);

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
        InternalAggregation metric = translator.toInternalAggregation(agg.getName(), value, AggregationTranslator.userMetadata(agg));
        return metric;
    }

    /**
     * Drains one granularity's rows into a re-readable list. The builder makes several passes over
     * them (filter, group, per-bucket recursion), so the executor's iterable is materialized once
     * per call.
     */
    private static List<Object[]> materialize(ExecutionResult result) {
        List<Object[]> rows = StreamSupport.stream(result.getRows().spliterator(), false).collect(Collectors.toList());
        return rows;
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
        List<String> accumulatedAggNames,
        Map<String, Object> parentKeyFilter
    ) throws ConversionException {

        GroupingInfo grouping = translator.getGrouping(agg);
        List<String> newAggNames = new ArrayList<>(accumulatedAggNames);
        newAggNames.add(agg.getName());

        ExecutionResult result = resultsByAggPath.get(pathKey(newAggNames));

        if (result == null) {
            return buildEmptyBucket(translator, agg);
        }

        List<Object[]> rows = materialize(result);

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
                : InternalAggregations.from(buildLevel(subAggs, newAggNames, childFilter));

            buckets.add(new BucketEntry(entry.getKey(), docCount, subAggregations));
        }

        Long eligibleDocCount = resolveEligibleDocCount(result, filteredRows, colIndex);
        if (eligibleDocCount != null && (translator instanceof SizedBucketTranslator) == false) {
            // Fetch is only granted to granularities defined by a SizedBucketTranslator; a
            // truncated plan rendered through the base contract would tail-sum a tail that
            // never left the engine.
            throw new ConversionException(
                "Plan for aggregation [" + agg.getName() + "] carried a fetch but its translator is not a SizedBucketTranslator"
            );
        }

        InternalAggregation bucketAgg = eligibleDocCount != null
            ? ((SizedBucketTranslator<AggregationBuilder>) translator).toBucketAggregation(agg, buckets, eligibleDocCount)
            : translator.toBucketAggregation(agg, buckets);
        return bucketAgg;
    }

    /**
     * Resolves the eligible-document total for a level whose plan is bounded. Root-level plans (a flat
     * LIMIT) take the aggregation's eligible-document count from the COUNT plans
     * ({@code COUNT(*)} when a {@code missing} value makes every matching doc eligible).
     * Nested levels (a per-parent bound) read the parent's eligible total off the rows — the
     * plan's window {@code SUM(_count) OVER (PARTITION BY parent)} rides every surviving row,
     * constant within the parent. Returns null for unbounded plans, where the translator's own
     * tail arithmetic is exact. A bounded level with no reachable eligible count is a wiring
     * bug — rendering would silently understate {@code sum_other_doc_count}, so it fails
     * loudly.
     */
    private Long resolveEligibleDocCount(ExecutionResult result, List<Object[]> filteredRows, Map<String, Integer> colIndex)
        throws ConversionException {
        AggregationMetadata metadata = result.getPlan().aggregationMetadata();

        if (metadata.getPerParentFetch() != null) {
            Integer eligibleIdx = colIndex.get(PostAggregateConverter.PARENT_ELIGIBLE_NAME);
            if (eligibleIdx == null) {
                throw new ConversionException(
                    "Nested bounded plan result is missing the "
                        + PostAggregateConverter.PARENT_ELIGIBLE_NAME
                        + " column for aggregation ["
                        + String.join(",", metadata.getAggNamePath())
                        + "]"
                );
            }
            if (filteredRows.isEmpty()) {
                return 0L;
            }
            Object eligible = filteredRows.get(0)[eligibleIdx];
            return eligible instanceof Number count ? count.longValue() : 0L;
        }

        if (metadata.getFetch() == null) {
            return null;
        }
        // a flat-LIMIT plan is root-level and single-field by eligibility
        String aggName = metadata.getAggNamePath().get(metadata.getAggNamePath().size() - 1);
        Long eligibleDocCount = null;
        if (countTotals != null) {
            eligibleDocCount = metadata.eligibleDocCountIsTotal() ? countTotals.totalDocs() : countTotals.eligibleDocCounts().get(aggName);
        }
        if (eligibleDocCount == null) {
            throw new ConversionException("COUNT plan result is missing the eligible-doc count for bounded aggregation [" + aggName + "]");
        }
        return eligibleDocCount;
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
     * Canonical plan key: aggregation names in nesting order (outer bucket first), NUL-joined.
     * Insertion uses the walker's {@link AggregationMetadata#getAggNamePath()} and lookup uses
     * the response walk's accumulated names — both are built in nesting order, so the key
     * matches by construction.
     */
    private static String pathKey(List<String> aggNames) {
        return AggregationMetadata.pathKey(aggNames);
    }
}
