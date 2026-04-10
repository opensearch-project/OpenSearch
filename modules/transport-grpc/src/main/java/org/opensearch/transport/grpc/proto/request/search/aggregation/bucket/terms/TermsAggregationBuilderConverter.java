/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.bucket.terms;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.SortOrder;
import org.opensearch.protobufs.SortOrderSingleMap;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.protobufs.TermsAggregationCollectMode;
import org.opensearch.protobufs.TermsAggregationExecutionHint;
import org.opensearch.protobufs.TermsAggregationFields;
import org.opensearch.protobufs.TermsInclude;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggregation.support.ValuesSourceAggregationProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggregation.support.ValuesSourceProtoFields;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;
import org.opensearch.transport.grpc.spi.AggregationBuilderProtoConverter;
import org.opensearch.transport.grpc.spi.AggregationBuilderProtoConverterRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converter from proto {@link TermsAggregation} to {@link TermsAggregationBuilder}
 */
public class TermsAggregationBuilderConverter implements AggregationBuilderProtoConverter {

    private AggregationBuilderProtoConverterRegistry registry;

    @Override
    public AggregationContainer.AggregationContainerCase getHandledAggregationCase() {
        return AggregationContainer.AggregationContainerCase.TERMS_AGGREGATION;
    }

    /**
     * This method parallels the REST parsing logic in {@link TermsAggregationBuilder#PARSER}
     *
     * {@inheritDoc}
     */
    @Override
    public AggregationBuilder fromProto(String name, AggregationContainer container) {
        if (!container.hasTermsAggregation()) {
            throw new IllegalStateException("AggregationContainer doesn't have terms aggregation");
        }
        TermsAggregation proto = container.getTermsAggregation();
        TermsAggregationBuilder aggBuilder = new TermsAggregationBuilder(name);

        if (!proto.hasTerms()) {
            throw new IllegalArgumentException("Terms fields are not provided, cannot parse");
        }

        TermsAggregationFields terms = proto.getTerms();

        // common fields for ValuesSourceAggregation
        ValuesSourceProtoFields valuesSourceFields = ValuesSourceProtoFields.builder()
            .field(terms.hasField() ? terms.getField() : null)
            .missing(terms.hasMissing() ? FieldValueProtoUtils.fromProto(terms.getMissing()) : null)
            .valueType(terms.hasValueType() ? terms.getValueType() : null)
            .format(terms.hasFormat() ? terms.getFormat() : null)
            .script(terms.hasScript() ? ScriptProtoUtils.parseFromProtoRequest(terms.getScript()) : null)
            .build();
        ValuesSourceAggregationProtoUtils.declareFields(aggBuilder, valuesSourceFields, true, true, false);

        if (terms.hasShowTermDocCountError()) {
            aggBuilder.showTermDocCountError(terms.getShowTermDocCountError());
        }

        if (terms.hasShardSize()) {
            aggBuilder.shardSize(terms.getShardSize());
        }

        if (terms.hasMinDocCount()) {
            aggBuilder.minDocCount(terms.getMinDocCount());
        }

        if (terms.hasShardMinDocCount()) {
            aggBuilder.shardMinDocCount(terms.getShardMinDocCount());
        }

        if (terms.hasSize()) {
            aggBuilder.size(terms.getSize());
        }

        if (terms.hasExecutionHint()) {
            aggBuilder.executionHint(convertExecutionHint(terms.getExecutionHint()));
        }

        if (terms.hasCollectMode()) {
            aggBuilder.collectMode(convertCollectMode(terms.getCollectMode()));
        }

        if (terms.getOrderCount() > 0) {
            aggBuilder.order(convertOrder(terms.getOrderList()));
        }

        // include/exclude handling mirrors IncludeExclude.merge logic in the REST parser
        IncludeExclude includeExclude = null;
        if (terms.hasInclude()) {
            includeExclude = convertInclude(terms.getInclude());
        }
        if (terms.getExcludeCount() > 0) {
            // See IncludeExclude::parseExclude
            String[] excludeValues = terms.getExcludeList().toArray(new String[0]);
            IncludeExclude excludeIE = new IncludeExclude(null, excludeValues);
            includeExclude = IncludeExclude.merge(includeExclude, excludeIE);
        }
        if (includeExclude != null) {
            aggBuilder.includeExclude(includeExclude);
        }

        // sub-aggregations
        if (proto.getAggregationsCount() > 0) {
            if (registry == null) {
                throw new IllegalStateException("Registry not set properly, cannot parse sub aggregations");
            }
            for (Map.Entry<String, AggregationContainer> entry : proto.getAggregationsMap().entrySet()) {
                aggBuilder.subAggregation(registry.fromProto(entry.getKey(), entry.getValue()));
            }
        }

        return aggBuilder;
    }

    private static String convertExecutionHint(TermsAggregationExecutionHint hint) {
        // TODO: Seems the schema is a bit off sync'd between proto and java impl? Enum in proto but string in java
        return switch (hint) {
            case TERMS_AGGREGATION_EXECUTION_HINT_MAP -> "map";
            case TERMS_AGGREGATION_EXECUTION_HINT_GLOBAL_ORDINALS -> "global_ordinals";
            case TERMS_AGGREGATION_EXECUTION_HINT_GLOBAL_ORDINALS_HASH -> "global_ordinals_hash";
            case TERMS_AGGREGATION_EXECUTION_HINT_GLOBAL_ORDINALS_LOW_CARDINALITY -> "global_ordinals_low_cardinality";
            default -> throw new IllegalArgumentException("Unsupported execution hint: " + hint);
        };
    }

    private static SubAggCollectionMode convertCollectMode(TermsAggregationCollectMode mode) {
        return switch (mode) {
            case TERMS_AGGREGATION_COLLECT_MODE_BREADTH_FIRST -> SubAggCollectionMode.BREADTH_FIRST;
            case TERMS_AGGREGATION_COLLECT_MODE_DEPTH_FIRST -> SubAggCollectionMode.DEPTH_FIRST;
            default -> throw new IllegalArgumentException("Unsupported collect mode: " + mode);
        };
    }

    private static List<BucketOrder> convertOrder(List<SortOrderSingleMap> orderList) {
        List<BucketOrder> orders = new ArrayList<>(orderList.size());
        for (SortOrderSingleMap entry : orderList) {
            boolean asc = entry.getSortOrder() == SortOrder.SORT_ORDER_ASC;
            String key = entry.getField();
            orders.add(InternalOrder.Parser.resolveOrderParam(key, asc));
        }
        return orders;
    }

    /**
     * See {@link IncludeExclude#parseInclude(XContentParser)}
     */
    private static IncludeExclude convertInclude(TermsInclude include) {
        return switch (include.getTermsIncludeCase()) {
            case TERMS -> {
                String[] includeTerms = include.getTerms().getStringArrayList().toArray(new String[0]);
                yield new IncludeExclude(includeTerms, null);
            }
            case PARTITION -> new IncludeExclude(include.getPartition().getPartition(), include.getPartition().getNumPartitions());
            default -> throw new IllegalArgumentException("Unsupported include type: " + include.getTermsIncludeCase());
        };
    }

    @Override
    public void setRegistry(AggregationBuilderProtoConverterRegistry registry) {
        this.registry = registry;
    }
}
