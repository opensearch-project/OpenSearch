/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.bucket.terms;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.protobufs.TermsAggregationCollectMode;
import org.opensearch.protobufs.TermsAggregationExecutionHint;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.transport.grpc.proto.request.search.aggregation.AggregationContainerProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggregation.BucketOrderProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggregation.support.ValuesSourceAggregationProtoUtils;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

import java.util.Map;

/**
 * Utility class for converting TermsAggregation Protocol Buffers to TermsAggregationBuilder objects.
 *
 * <p>Field processing follows the exact sequence defined in {@link TermsAggregationBuilder#PARSER}
 * to ensure identical behavior with REST API parsing.
 *
 * @see TermsAggregationBuilder#PARSER
 * @see org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder#declareFields
 */
public class TermsAggregationProtoUtils {

    private TermsAggregationProtoUtils() {
        // Utility class
    }

    /**
     * Converts a Protocol Buffer TermsAggregation to a TermsAggregationBuilder.
     *
     * <p>Mirrors {@link TermsAggregationBuilder#PARSER}, processing fields in the exact same sequence
     * to ensure consistent validation and behavior.
     *
     * @param name The name of the aggregation (from parent container map key)
     * @param proto The Protocol Buffer TermsAggregation to convert
     * @return A configured TermsAggregationBuilder
     * @throws IllegalArgumentException if required fields are missing or validation fails
     */
    public static TermsAggregationBuilder fromProto(String name, TermsAggregation proto) {
        if (proto == null) {
            throw new IllegalArgumentException("TermsAggregation proto must not be null");
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Aggregation name must not be null or empty");
        }

        TermsAggregationBuilder builder = new TermsAggregationBuilder(name);

        // ========================================
        // STEP 1: ValuesSourceAggregationBuilder common fields
        // ========================================
        // Mirrors ValuesSourceAggregationBuilder#declareFields
        // For terms aggregation: scriptable=true, formattable=true, timezoneAware=false, fieldRequired=true

        ValuesSourceAggregationProtoUtils.parseField(builder, proto.hasField(), proto.getField());
        ValuesSourceAggregationProtoUtils.parseMissing(builder, proto.hasMissing(), proto.getMissing());
        ValuesSourceAggregationProtoUtils.parseValueType(builder, proto.hasValueType(), proto.getValueType());

        // Conditional fields based on configuration
        ValuesSourceAggregationProtoUtils.parseConditionalFields(
            builder,
            proto.hasFormat(),
            proto.getFormat(),
            proto.hasScript(),
            proto.getScript(),
            proto.hasField() && !proto.getField().isEmpty(),
            /* scriptable= */ true,
            /* formattable= */ true,
            /* timezoneAware= */ false,
            /* fieldRequired= */ true
        );

        // ========================================
        // STEP 2: TermsAggregationBuilder specific fields (in REST declaration order)
        // ========================================
        // Mirrors TermsAggregationBuilder.PARSER static block

        // show_term_doc_count_error
        if (proto.hasShowTermDocCountError()) {
            builder.showTermDocCountError(proto.getShowTermDocCountError());
        }

        // shard_size
        if (proto.hasShardSize()) {
            builder.shardSize(proto.getShardSize());
        }

        // min_doc_count
        if (proto.hasMinDocCount()) {
            builder.minDocCount(proto.getMinDocCount());
        }

        // shard_min_doc_count
        if (proto.hasShardMinDocCount()) {
            builder.shardMinDocCount(proto.getShardMinDocCount());
        }

        // size
        if (proto.hasSize()) {
            builder.size(proto.getSize());
        }

        // execution_hint
        // Mirrors TermsAggregationBuilder#executionHint(String) which accepts any string without validation
        if (proto.hasExecutionHint()) {
            String hint = ProtobufEnumUtils.convertToString(proto.getExecutionHint());
            if (hint != null) {
                builder.executionHint(hint);
            }
        }

        // collect_mode
        // Mirrors Aggregator.SubAggCollectionMode#parse for validation
        if (proto.hasCollectMode()) {
            String modeStr = ProtobufEnumUtils.convertToString(proto.getCollectMode());
            if (modeStr != null) {
                Aggregator.SubAggCollectionMode mode = Aggregator.SubAggCollectionMode.parse(
                    modeStr,
                    org.opensearch.common.xcontent.LoggingDeprecationHandler.INSTANCE
                );
                builder.collectMode(mode);
            }
        }

        // order
        BucketOrderProtoUtils.toProto(builder, proto.getOrderList());

        // include
        // Mirrors IncludeExclude#merge pattern
        if (proto.hasInclude()) {
            IncludeExclude include = IncludeExcludeProtoUtils.parseInclude(proto.getInclude());
            builder.includeExclude(IncludeExclude.merge(include, builder.includeExclude()));
        }

        // exclude
        // Mirrors IncludeExclude#merge pattern
        if (proto.getExcludeCount() > 0) {
            IncludeExclude exclude = IncludeExcludeProtoUtils.parseExclude(proto.getExcludeList());
            builder.includeExclude(IncludeExclude.merge(builder.includeExclude(), exclude));
        }

        // ========================================
        // STEP 3: Sub-aggregations
        // ========================================
        // Mirrors AggregatorFactories#parseAggregators
        // Sub-aggregations are processed AFTER all other fields to match REST behavior

        for (Map.Entry<String, AggregationContainer> entry : proto.getAggregationsMap().entrySet()) {
            AggregationBuilder subAgg = AggregationContainerProtoUtils.fromProto(entry.getKey(), entry.getValue());
            builder.subAggregation(subAgg);
        }

        return builder;
    }
}
