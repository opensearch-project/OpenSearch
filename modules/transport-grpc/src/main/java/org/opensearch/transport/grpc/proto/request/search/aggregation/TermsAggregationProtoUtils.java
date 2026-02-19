/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.protobufs.TermsAggregationCollectMode;
import org.opensearch.protobufs.TermsAggregationExecutionHint;
import org.opensearch.protobufs.TermsInclude;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Utility class for converting TermsAggregation protocol buffers to OpenSearch TermsAggregationBuilder objects.
 *
 * <p>This class mirrors the REST-side parsing logic for TermsAggregation,
 * translating protobuf fields to their corresponding builder methods.
 *
 * <p><b>Note:</b> Nested sub-aggregations are not supported in this implementation.
 * The protobuf schema does not include an aggregations field in bucket response messages,
 * making it impossible to return nested aggregation results. Therefore, nested aggregations
 * are intentionally not parsed from requests.
 *
 * <p>The TermsAggregation supports:
 * <ul>
 *   <li>Field or script-based value sources</li>
 *   <li>Bucket sizing and ordering</li>
 *   <li>Include/exclude patterns</li>
 *   <li>Execution hints and collection modes</li>
 * </ul>
 */
class TermsAggregationProtoUtils {

    private TermsAggregationProtoUtils() {
        // Utility class
    }

    /**
     * Converts a protobuf TermsAggregation to an OpenSearch TermsAggregationBuilder.
     *
     * <p>This method processes all fields from the protobuf message and constructs a fully
     * configured TermsAggregationBuilder.
     *
     * @param name The name of the aggregation
     * @param termsAggProto The protobuf TermsAggregation message
     * @return A configured TermsAggregationBuilder
     * @throws IllegalArgumentException if neither field nor script is provided, or on conversion errors
     */
    static TermsAggregationBuilder fromProto(String name, TermsAggregation termsAggProto) {
        if (termsAggProto == null) {
            throw new IllegalArgumentException("TermsAggregation proto must not be null");
        }

        // Create base builder
        TermsAggregationBuilder builder = new TermsAggregationBuilder(name);

        // 1. Core fields (field or script) - at least one is required
        if (termsAggProto.hasField()) {
            builder.field(termsAggProto.getField());
        }

        if (termsAggProto.hasScript()) {
            try {
                Script script = ScriptProtoUtils.parseFromProtoRequest(termsAggProto.getScript());
                builder.script(script);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse script for terms aggregation", e);
            }
        }

        // Validate: must have either field or script
        if (!termsAggProto.hasField() && !termsAggProto.hasScript()) {
            throw new IllegalArgumentException("Terms aggregation must specify either 'field' or 'script'");
        }

        // 2. Missing value
        if (termsAggProto.hasMissing()) {
            Object missingValue = FieldValueProtoUtils.fromProto(termsAggProto.getMissing());
            builder.missing(missingValue);
        }

        // 3. Bucket thresholds
        if (termsAggProto.hasSize()) {
            builder.size(termsAggProto.getSize());
        }

        if (termsAggProto.hasShardSize()) {
            builder.shardSize(termsAggProto.getShardSize());
        }

        if (termsAggProto.hasMinDocCount()) {
            builder.minDocCount(termsAggProto.getMinDocCount());
        }

        if (termsAggProto.hasShardMinDocCount()) {
            builder.shardMinDocCount(termsAggProto.getShardMinDocCount());
        }

        // 4. Display options
        if (termsAggProto.hasShowTermDocCountError()) {
            builder.showTermDocCountError(termsAggProto.getShowTermDocCountError());
        }

        // 5. Execution hint
        if (termsAggProto.hasExecutionHint()) {
            String hint = convertExecutionHint(termsAggProto.getExecutionHint());
            if (hint != null) {
                builder.executionHint(hint);
            }
        }

        // 6. Bucket ordering
        if (termsAggProto.getOrderCount() > 0) {
            List<BucketOrder> orders = new ArrayList<>();
            for (Map.Entry<String, org.opensearch.protobufs.SortOrder> entry : termsAggProto.getOrderMap().entrySet()) {
                boolean asc = entry.getValue() == org.opensearch.protobufs.SortOrder.SORT_ORDER_ASC;
                orders.add(convertBucketOrder(entry.getKey(), asc));
            }

            // Use compound order if multiple, otherwise single order
            if (orders.size() == 1) {
                builder.order(orders.get(0));
            } else if (orders.size() > 1) {
                builder.order(BucketOrder.compound(orders));
            }
        }

        // 7. Collection mode
        if (termsAggProto.hasCollectMode()) {
            Aggregator.SubAggCollectionMode mode = convertCollectMode(termsAggProto.getCollectMode());
            if (mode != null) {
                builder.collectMode(mode);
            }
        }

        // 8. Include/Exclude patterns
        IncludeExclude includeExclude = convertIncludeExclude(
            termsAggProto.hasInclude() ? termsAggProto.getInclude() : null,
            termsAggProto.getExcludeList()
        );
        if (includeExclude != null) {
            builder.includeExclude(includeExclude);
        }

        // 9. Value type for unmapped fields
        if (termsAggProto.hasValueType()) {
            ValueType valueType = ValueType.lenientParse(termsAggProto.getValueType());
            if (valueType != null) {
                builder.userValueTypeHint(valueType);
            }
        }

        // 10. Output format
        if (termsAggProto.hasFormat()) {
            builder.format(termsAggProto.getFormat());
        }

        return builder;
    }

    /**
     * Converts a bucket order key and direction to a BucketOrder.
     * Special keys like "_count" and "_key" are handled, otherwise treated as sub-aggregation/metric ordering.
     */
    private static BucketOrder convertBucketOrder(String key, boolean asc) {
        if ("_count".equals(key)) {
            return asc ? BucketOrder.count(true) : BucketOrder.count(false);
        } else if ("_key".equals(key) || "_term".equals(key)) {
            return asc ? BucketOrder.key(true) : BucketOrder.key(false);
        } else {
            // Metric ordering (e.g., ordering by average value from a metric aggregation)
            // Note: Sub-aggregations are not supported in this implementation
            return asc ? BucketOrder.aggregation(key, true) : BucketOrder.aggregation(key, false);
        }
    }

    /**
     * Converts protobuf TermsInclude and exclude list to OpenSearch IncludeExclude.
     * Supports both regex patterns and partition-based filtering.
     */
    private static IncludeExclude convertIncludeExclude(TermsInclude includeProto, List<String> excludeList) {
        String[] includeValues = null;
        String[] excludeValues = excludeList != null && !excludeList.isEmpty() ? excludeList.toArray(new String[0]) : null;

        if (includeProto != null) {
            switch (includeProto.getTermsIncludeCase()) {
                case TERMS:
                    if (includeProto.getTerms().getStringArrayCount() > 0) {
                        includeValues = includeProto.getTerms().getStringArrayList().toArray(new String[0]);
                    }
                    break;
                case PARTITION:
                    // Partition-based filtering
                    int numPartitions = (int) includeProto.getPartition().getNumPartitions();
                    int partition = (int) includeProto.getPartition().getPartition();
                    return new IncludeExclude(partition, numPartitions);
                case TERMSINCLUDE_NOT_SET:
                    break;
            }
        }

        if (includeValues != null || excludeValues != null) {
            return new IncludeExclude(includeValues, excludeValues);
        }

        return null;
    }

    /**
     * Converts protobuf execution hint enum to string.
     * Filters out UNSPECIFIED values.
     */
    private static String convertExecutionHint(TermsAggregationExecutionHint hint) {
        if (hint == null || hint == TermsAggregationExecutionHint.TERMS_AGGREGATION_EXECUTION_HINT_UNSPECIFIED) {
            return null;
        }
        // Convert enum name to lowercase (e.g., GLOBAL_ORDINALS -> global_ordinals)
        return hint.name().toLowerCase(Locale.ROOT).replace("terms_aggregation_execution_hint_", "");
    }

    /**
     * Converts protobuf collection mode enum to OpenSearch SubAggCollectionMode.
     */
    private static Aggregator.SubAggCollectionMode convertCollectMode(TermsAggregationCollectMode mode) {
        if (mode == null || mode == TermsAggregationCollectMode.TERMS_AGGREGATION_COLLECT_MODE_UNSPECIFIED) {
            return null;
        }

        switch (mode) {
            case TERMS_AGGREGATION_COLLECT_MODE_BREADTH_FIRST:
                return Aggregator.SubAggCollectionMode.BREADTH_FIRST;
            case TERMS_AGGREGATION_COLLECT_MODE_DEPTH_FIRST:
                return Aggregator.SubAggCollectionMode.DEPTH_FIRST;
            default:
                return null;
        }
    }
}
