/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Mutable builder for {@link AggregationMetadata}. Used by {@link AggregationTreeWalker}
 * to accumulate groupings and aggregate calls during tree traversal.
 * Grouping indices are resolved at build time from the input row type.
 */
public class AggregationMetadataBuilder {

    /** Name used for the implicit COUNT(*) aggregate added by bucket aggregations. */
    public static final String IMPLICIT_COUNT_NAME = "_count";

    private final List<GroupingInfo> groupings = new ArrayList<>();
    private final List<AggregateCall> aggregateCalls = new ArrayList<>();
    private final List<String> aggregateFieldNames = new ArrayList<>();
    private final List<PipelineAggregationBuilder> pipelineBuilders = new ArrayList<>();
    private final Map<String, String> aggNameToGranularityKeySegment = new HashMap<>();
    private boolean implicitCountRequested = false;
    private RexNode filterCondition;
    private String bucketKey;
    private String aggregationName;

    /** Creates a new empty builder. */
    public AggregationMetadataBuilder() {}

    /**
     * Adds a grouping contribution from a bucket translator.
     *
     * @param grouping the grouping info
     */
    public void addGrouping(GroupingInfo grouping) {
        groupings.add(grouping);
    }

    /**
     * Adds an aggregate call with its output field name.
     *
     * @param call the Calcite aggregate call
     * @param fieldName the output field name
     */
    public void addAggregateCall(AggregateCall call, String fieldName) {
        aggregateCalls.add(call);
        aggregateFieldNames.add(fieldName);
    }

    /**
     * Requests an implicit COUNT(*) for bucket doc_count.
     * Idempotent — only one COUNT(*) is created at build time.
     */
    public void requestImplicitCount() {
        this.implicitCountRequested = true;
    }

    /**
     * Sets the filter condition from a filter bucket aggregation.
     *
     * @param filterCondition the RexNode filter condition, or null for no filter
     */
    public void setFilterCondition(RexNode filterCondition) {
        this.filterCondition = filterCondition;
    }

    /**
     * Sets the bucket key for response assembly (e.g., filter key in a filters aggregation).
     *
     * @param bucketKey the bucket key, or null for no bucket key
     */
    public void setBucketKey(String bucketKey) {
        this.bucketKey = bucketKey;
    }

    /**
     * Sets the DSL aggregation name for response assembly.
     *
     * @param aggregationName the aggregation name from the DSL, or null
     */
    public void setAggregationName(String aggregationName) {
        this.aggregationName = aggregationName;
    }

    /**
     * Adds a pipeline aggregation builder collected during tree walk.
     *
     * @param pipelineBuilder the pipeline aggregation builder
     */
    public void addPipelineBuilder(PipelineAggregationBuilder pipelineBuilder) {
        pipelineBuilders.add(pipelineBuilder);
    }

    /**
     * Records the mapping from a bucket aggregation's DSL name to its granularity key segment.
     * For standard buckets (terms), the segment is the comma-joined field names (e.g., "brand").
     * For filter buckets, the segment is the filter key suffix (e.g., "__filter__expensive_only").
     *
     * @param aggName the DSL aggregation name (e.g., "by_region" or "filter_expensive")
     * @param keySegment the granularity key segment this agg contributes
     */
    public void addAggNameMapping(String aggName, String keySegment) {
        aggNameToGranularityKeySegment.put(aggName, keySegment);
    }

    /**
     * Copies all agg-name-to-key-segment mappings from this builder into the given map.
     * Existing entries in the target map are not overwritten.
     *
     * @param target the map to populate
     */
    public void collectAggNameMappings(Map<String, String> target) {
        aggNameToGranularityKeySegment.forEach(target::putIfAbsent);
    }

    /** Returns true if this builder has at least one aggregate call, implicit count, or pipeline builder. */
    public boolean hasAggregateCalls() {
        return !aggregateCalls.isEmpty() || implicitCountRequested;
    }

    /**
     * Returns true if this builder contains an aggregate field with the given name.
     *
     * @param fieldName the field name to check
     * @return true if the field exists in the aggregate field names
     */
    public boolean containsAggregateField(String fieldName) {
        return aggregateFieldNames.contains(fieldName);
    }

    /**
     * Builds the immutable metadata. Resolves grouping indices from the input row type.
     * For no-GROUP-BY metrics, makes return types nullable (AVG of empty set is null).
     *
     * @param inputRowType the schema before aggregation
     * @param typeFactory the type factory for creating types
     * @return the aggregation metadata
     * @throws ConversionException if field resolution fails
     */
    public AggregationMetadata build(RelDataType inputRowType, RelDataTypeFactory typeFactory) throws ConversionException {
        // Resolve grouping indices at build time
        List<Integer> allGroupIndices = new ArrayList<>();
        List<String> allGroupFieldNames = new ArrayList<>();
        for (GroupingInfo g : groupings) {
            allGroupIndices.addAll(g.resolveIndices(inputRowType));
            allGroupFieldNames.addAll(g.getFieldNames());
        }

        // For no-GROUP-BY, metric results could be null (e.g., AVG of empty set).
        // COUNT stays non-nullable (returns 0).
        boolean noGroupBy = groupings.isEmpty();
        List<AggregateCall> allCalls = new ArrayList<>();
        for (AggregateCall call : aggregateCalls) {
            if (noGroupBy) {
                RelDataType nullableType = typeFactory.createTypeWithNullability(call.getType(), true);
                allCalls.add(AggregateCall.create(
                    call.getAggregation(), call.isDistinct(), call.isApproximate(),
                    call.ignoreNulls(), call.getArgList(), call.filterArg,
                    call.getCollation(), nullableType, call.getName()
                ));
            } else {
                allCalls.add(call);
            }
        }
        List<String> allFieldNames = new ArrayList<>(aggregateFieldNames);

        if (implicitCountRequested) {
            allCalls.add(AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                false,
                false,
                false,
                List.of(),
                -1,
                RelCollations.EMPTY,
                typeFactory.createSqlType(SqlTypeName.BIGINT),
                IMPLICIT_COUNT_NAME
            ));
            allFieldNames.add(IMPLICIT_COUNT_NAME);
        }

        return new AggregationMetadata(
            ImmutableBitSet.of(allGroupIndices),
            allGroupFieldNames,
            allCalls,
            allFieldNames,
            filterCondition,
            bucketKey,
            aggregationName,
            pipelineBuilders
        );
    }
}
