/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.StringMap;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.protobufs.TermsAggregationCollectMode;
import org.opensearch.protobufs.TermsAggregationExecutionHint;
import org.opensearch.protobufs.TermsInclude;
import org.opensearch.protobufs.TermsPartition;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting TermsAggregation Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of terms aggregations
 * into their corresponding OpenSearch TermsAggregationBuilder implementations.
 */
public class TermsAggregationProtoUtils {

    private TermsAggregationProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer TermsAggregation to an OpenSearch TermsAggregationBuilder.
     *
     * <p>This method is the gRPC equivalent of {@link TermsAggregationBuilder#PARSER}, which parses
     * terms aggregations from REST/JSON requests via {@code fromXContent}. Similar to how the parser
     * reads JSON fields, this method extracts values from the Protocol Buffer representation and creates
     * a properly configured TermsAggregationBuilder with field name, size, order, include/exclude patterns,
     * execution hints, collection mode, and nested sub-aggregations.
     *
     * <p>The REST-side serialization via {@link TermsAggregationBuilder#doXContentBody} produces
     * JSON that conceptually mirrors the protobuf structure used here.
     *
     * @param name The name of the aggregation
     * @param termsAggProto The Protocol Buffer TermsAggregation object
     * @return A configured TermsAggregationBuilder instance
     * @throws IllegalArgumentException if there's an error parsing the aggregation
     * @see TermsAggregationBuilder#PARSER
     * @see TermsAggregationBuilder#doXContentBody
     */
    public static TermsAggregationBuilder fromProto(String name, TermsAggregation termsAggProto) {
        TermsAggregationBuilder builder = new TermsAggregationBuilder(name);

        // Parse in same order as TermsAggregationBuilder.doXContentBody()

        // 1. field, missing, script (from ValuesSourceAggregationBuilder parent class)
        if (termsAggProto.hasField()) {
            builder.field(termsAggProto.getField());
        }

        if (termsAggProto.hasMissing()) {
            FieldValue missingValue = termsAggProto.getMissing();
            Object missing = FieldValueProtoUtils.fromProto(missingValue, false);
            builder.missing(missing);
        }

        if (termsAggProto.hasScript()) {
            try {
                builder.script(ScriptProtoUtils.parseFromProtoRequest(termsAggProto.getScript()));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse script for terms aggregation", e);
            }
        }

        // 2. bucketCountThresholds: size, shard_size, min_doc_count, shard_min_doc_count
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

        // 3. show_term_doc_count_error
        if (termsAggProto.hasShowTermDocCountError()) {
            builder.showTermDocCountError(termsAggProto.getShowTermDocCountError());
        }

        // 4. execution_hint
        if (termsAggProto.hasExecutionHint()) {
            String executionHint = convertExecutionHint(termsAggProto.getExecutionHint());
            if (executionHint != null) {
                builder.executionHint(executionHint);
            }
        }

        // 5. order
        if (termsAggProto.getOrderCount() > 0) {
            List<BucketOrder> orders = new ArrayList<>();
            for (StringMap orderMap : termsAggProto.getOrderList()) {
                for (Map.Entry<String, String> entry : orderMap.getStringMapMap().entrySet()) {
                    String key = entry.getKey();
                    String direction = entry.getValue();
                    boolean asc = "asc".equalsIgnoreCase(direction);

                    BucketOrder order = convertBucketOrder(key, asc);
                    orders.add(order);
                }
            }
            if (orders.size() == 1) {
                builder.order(orders.get(0));
            } else if (orders.size() > 1) {
                builder.order(BucketOrder.compound(orders));
            }
        }

        // 6. collect_mode
        if (termsAggProto.hasCollectMode()) {
            Aggregator.SubAggCollectionMode collectMode = convertCollectMode(termsAggProto.getCollectMode());
            if (collectMode != null) {
                builder.collectMode(collectMode);
            }
        }

        // 7. include/exclude
        IncludeExclude includeExclude = convertIncludeExclude(
            termsAggProto.hasInclude() ? termsAggProto.getInclude() : null,
            termsAggProto.getExcludeList()
        );
        if (includeExclude != null) {
            builder.includeExclude(includeExclude);
        }

        // 8. value_type (for type coercion of unmapped fields)
        if (termsAggProto.hasValueType()) {
            ValueType valueType = ValueType.lenientParse(termsAggProto.getValueType());
            if (valueType != null) {
                builder.userValueTypeHint(valueType);
            }
        }

        // 9. format (for formatting output values)
        if (termsAggProto.hasFormat()) {
            builder.format(termsAggProto.getFormat());
        }

        // Handle sub-aggregations if present
        if (termsAggProto.getAggregationsCount() > 0) {
            Map<String, AggregationContainer> subAggs = termsAggProto.getAggregationsMap();
            for (Map.Entry<String, AggregationContainer> entry : subAggs.entrySet()) {
                AggregationBuilder subAggBuilder = AggregationContainerProtoUtils.fromProto(entry.getKey(), entry.getValue());
                builder.subAggregation(subAggBuilder);
            }
        }
        // Also handle aggs (alias for aggregations)
        if (termsAggProto.getAggsCount() > 0) {
            Map<String, AggregationContainer> subAggs = termsAggProto.getAggsMap();
            for (Map.Entry<String, AggregationContainer> entry : subAggs.entrySet()) {
                AggregationBuilder subAggBuilder = AggregationContainerProtoUtils.fromProto(entry.getKey(), entry.getValue());
                builder.subAggregation(subAggBuilder);
            }
        }

        return builder;
    }

    /**
     * Converts a key and direction to an OpenSearch BucketOrder.
     *
     * @param key The order key
     * @param asc Whether to sort in ascending order
     * @return The OpenSearch BucketOrder
     */
    private static BucketOrder convertBucketOrder(String key, boolean asc) {
        // Handle special keys
        if ("_count".equals(key)) {
            return BucketOrder.count(asc);
        } else if ("_key".equals(key) || "_term".equals(key)) {
            return BucketOrder.key(asc);
        } else {
            // Sub-aggregation or metric ordering
            return BucketOrder.aggregation(key, asc);
        }
    }

    /**
     * Converts a Protocol Buffer TermsInclude and exclude list to an OpenSearch IncludeExclude.
     *
     * @param includeProto The Protocol Buffer TermsInclude
     * @param excludeList The list of exclude patterns
     * @return The OpenSearch IncludeExclude, or null if empty
     */
    private static IncludeExclude convertIncludeExclude(TermsInclude includeProto, List<String> excludeList) {
        // Handle partition-based include/exclude
        if (includeProto != null && includeProto.hasPartition()) {
            TermsPartition partition = includeProto.getPartition();
            return new IncludeExclude((int) partition.getPartition(), (int) partition.getNumPartitions());
        }

        // Handle string array includes and excludes
        String[] includeArray = null;
        if (includeProto != null && includeProto.hasStringArray()) {
            List<String> includeList = includeProto.getStringArray().getStringArrayList();
            if (!includeList.isEmpty()) {
                includeArray = includeList.toArray(new String[0]);
            }
        }

        String[] excludeArray = null;
        if (excludeList != null && !excludeList.isEmpty()) {
            excludeArray = excludeList.toArray(new String[0]);
        }

        if (includeArray != null || excludeArray != null) {
            return new IncludeExclude(includeArray, excludeArray);
        }

        return null;
    }

    /**
     * Converts a Protocol Buffer TermsAggregationExecutionHint to its string representation.
     *
     * @param hint The Protocol Buffer execution hint
     * @return The string representation of the execution hint, or null if unspecified
     */
    private static String convertExecutionHint(TermsAggregationExecutionHint hint) {
        if (hint == TermsAggregationExecutionHint.TERMS_AGGREGATION_EXECUTION_HINT_UNSPECIFIED) {
            return null;
        }
        return ProtobufEnumUtils.convertToString(hint);
    }

    /**
     * Converts a Protocol Buffer TermsAggregationCollectMode to an OpenSearch SubAggCollectionMode.
     *
     * @param mode The Protocol Buffer collect mode
     * @return The OpenSearch SubAggCollectionMode, or null if unspecified
     */
    private static Aggregator.SubAggCollectionMode convertCollectMode(TermsAggregationCollectMode mode) {
        switch (mode) {
            case TERMS_AGGREGATION_COLLECT_MODE_BREADTH_FIRST:
                return Aggregator.SubAggCollectionMode.BREADTH_FIRST;
            case TERMS_AGGREGATION_COLLECT_MODE_DEPTH_FIRST:
                return Aggregator.SubAggCollectionMode.DEPTH_FIRST;
            case TERMS_AGGREGATION_COLLECT_MODE_UNSPECIFIED:
            default:
                return null;
        }
    }
}
