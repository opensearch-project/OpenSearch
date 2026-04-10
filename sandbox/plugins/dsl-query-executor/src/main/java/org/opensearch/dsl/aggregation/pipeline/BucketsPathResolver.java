/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.pipeline;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.support.AggregationPath;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves {@code buckets_path} references to column indices in a RelNode's output row type.
 *
 * <p>Uses {@link AggregationPath} to parse the path, which handles all OpenSearch path syntax:
 * <ul>
 *   <li>{@code >} for aggregation nesting (e.g., {@code "sales_per_brand>sum_of_price"})</li>
 *   <li>{@code .} for metric properties (e.g., {@code "extended_stats_agg.std_deviation"})</li>
 *   <li>{@code [key]} for bucket keys (e.g., {@code "my_terms[some_key]"})</li>
 *   <li>{@code _count} and {@code _key} as special built-in references</li>
 * </ul>
 */
public class BucketsPathResolver {

    private BucketsPathResolver() {}

    /**
     * Resolves a {@code buckets_path} to a column index in the RelNode's output row type.
     *
     * <p>Parses the path using {@link AggregationPath#parse(String)} and extracts the last
     * path element's name as the column to look up. For sibling pipeline aggregations, the
     * last element is the metric name (e.g., {@code "sum_of_price"} from
     * {@code "sales_per_brand>sum_of_price"}). For property paths, the key holds the
     * property name (e.g., {@code "std_deviation"} from {@code "extended_stats_agg.std_deviation"}).
     *
     * @param path    the buckets_path string
     * @param relNode the RelNode whose output row type to search
     * @return the zero-based column index
     * @throws ConversionException if the path cannot be resolved
     */
    public static int resolve(String path, RelNode relNode) throws ConversionException {
        AggregationPath aggPath = AggregationPath.parse(path);
        AggregationPath.PathElement lastElement = aggPath.lastPathElement();

        // If the last element has a key (from '.' property syntax like "stats.avg"),
        // use the full "name.key" as the column name; otherwise use just the name.
        String columnName;
        if (lastElement.key != null) {
            columnName = lastElement.name + "." + lastElement.key;
        } else {
            columnName = lastElement.name;
        }

        RelDataType rowType = relNode.getRowType();
        RelDataTypeField field = rowType.getField(columnName, false, false);
        if (field == null) {
            // Fall back: try just the key (property name) for stats-style metrics
            // where the column might be named "std_deviation" rather than "stats.std_deviation"
            if (lastElement.key != null) {
                field = rowType.getField(lastElement.key, false, false);
            }
        }
        if (field == null) {
            throw new ConversionException(
                "Pipeline buckets_path '" + path + "' could not be resolved. "
                    + "Available columns: " + rowType.getFieldNames()
            );
        }
        return field.getIndex();
    }

    /**
     * Finds the granularity builder that contains the metric referenced by a pipeline
     * aggregation's {@code buckets_path}. Combines the current grouping field names
     * (from parent bucket aggregations) with the intermediate path elements resolved
     * to their grouping field names, then constructs the expected granularity key.
     *
     * <p>For a top-level pipeline with {@code buckets_path = "by_region>by_brand>total_revenue"},
     * current groupings are empty, and the key is built entirely from the path: {@code "region,brand"}.
     *
     * <p>For a nested pipeline inside {@code by_region} with {@code buckets_path = "by_brand>total_revenue"},
     * current groupings provide {@code ["region"]} and the path adds {@code ["brand"]},
     * producing key {@code "region,brand"}.
     *
     * @param pipelineBuilder  the pipeline aggregation builder
     * @param currentGroupings accumulated groupings from parent bucket aggregations
     * @param granularities    the map of granularity keys to metadata builders
     * @return the matching builder
     */
    public static AggregationMetadataBuilder findBuilderForPipeline(
        PipelineAggregationBuilder pipelineBuilder,
        List<GroupingInfo> currentGroupings,
        Map<String, AggregationMetadataBuilder> granularities
    ) {
        String[] bucketsPaths = pipelineBuilder.getBucketsPaths();
        if (bucketsPaths != null && bucketsPaths.length > 0) {
            List<AggregationPath.PathElement> pathElements =
                AggregationPath.parse(bucketsPaths[0]).getPathElements();

            if (pathElements.size() > 1) {
                Map<String, String> aggNameLookup = new LinkedHashMap<>();
                for (AggregationMetadataBuilder candidate : granularities.values()) {
                    candidate.collectAggNameMappings(aggNameLookup);
                }

                // Build the expected granularity key by joining all segments with commas
                List<String> segments = new ArrayList<>();
                // Start with current grouping field names as prefix
                for (GroupingInfo g : currentGroupings) {
                    segments.addAll(g.getFieldNames());
                }
                // Append key segment from each intermediate path element
                for (int i = 0; i < pathElements.size() - 1; i++) {
                    String segment = aggNameLookup.get(pathElements.get(i).name);
                    if (segment != null) {
                        segments.add(segment);
                    }
                }
                AggregationMetadataBuilder matched = granularities.get(String.join(",", segments));
                if (matched != null) {
                    return matched;
                }
            }

            // Single-level path: use the current groupings builder
            if (!currentGroupings.isEmpty()) {
                List<String> fieldNames = new ArrayList<>();
                for (GroupingInfo g : currentGroupings) {
                    fieldNames.addAll(g.getFieldNames());
                }
                AggregationMetadataBuilder matched = granularities.get(String.join(",", fieldNames));
                if (matched != null && matched.containsAggregateField(
                        pathElements.getLast().name)) {
                    return matched;
                }
            }

            // Search all granularities by metric name
            String metricName = pathElements.getLast().name;
            for (AggregationMetadataBuilder candidate : granularities.values()) {
                if (candidate.containsAggregateField(metricName)) {
                    return candidate;
                }
            }
        }
        return granularities.values().iterator().next();
    }
}
