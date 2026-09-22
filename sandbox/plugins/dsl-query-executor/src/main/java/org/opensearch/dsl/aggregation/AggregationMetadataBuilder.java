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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Mutable builder for {@link AggregationMetadata}. Used by {@link AggregationTreeWalker}
 * to accumulate the defining bucket aggregation's plan parameters, parent groupings, and
 * metric calls during tree traversal. Grouping indices are resolved at build time from the
 * input row type.
 *
 * <p>One builder exists per aggregation-name path — plans are per aggregation, so exactly one
 * bucket aggregation defines each builder (none for the global metrics builder).
 */
public class AggregationMetadataBuilder {

    /** Name used for the implicit COUNT(*) aggregate added by bucket aggregations. */
    public static final String IMPLICIT_COUNT_NAME = "_count";

    private final List<String> aggNamePath;
    private final List<GroupingInfo> groupings = new ArrayList<>();
    private final Map<String, Object> missingValues = new LinkedHashMap<>();
    private final List<AggregateCall> aggregateCalls = new ArrayList<>();
    private final List<String> aggregateFieldNames = new ArrayList<>();
    private final List<BucketOrder> bucketOrders = new ArrayList<>();
    private Integer definingSize;
    private Long definingMinDocCount;
    private boolean implicitCountRequested = false;

    /** Creates a builder for the global (no defining aggregation) metrics plan. */
    public AggregationMetadataBuilder() {
        this(List.of());
    }

    /**
     * Creates a builder for the plan defined by the given aggregation-name path.
     *
     * @param aggNamePath the defining aggregation-name path, outer bucket first
     */
    public AggregationMetadataBuilder(List<String> aggNamePath) {
        this.aggNamePath = List.copyOf(aggNamePath);
    }

    /**
     * Adds a grouping contribution from a bucket translator. The grouping's per-field
     * {@code missing} values are accumulated for the plan's null handling.
     *
     * @param grouping the grouping info
     */
    public void addGrouping(GroupingInfo grouping) {
        groupings.add(grouping);
        missingValues.putAll(grouping.getMissingByField());
    }

    /**
     * Adds a bucket order for post-aggregation sorting.
     * Compound orders are flattened into individual elements.
     *
     * @param order the bucket order
     */
    private void addBucketOrder(BucketOrder order) {
        if (order == null) return;
        if (order instanceof InternalOrder.CompoundOrder compound) {
            bucketOrders.addAll(compound.orderElements());
        } else {
            bucketOrders.add(order);
        }
    }

    /**
     * Records the plan parameters of the bucket aggregation that defines this plan: its sort
     * order plus the {@code size} and {@code min_doc_count} to bake in as LIMIT and HAVING.
     * Called exactly once per builder — each bucket aggregation owns its own plan.
     *
     * @param order the bucket order (flattened like {@link #addBucketOrder})
     * @param size the requested bucket count, or null for base-contract bucket types
     * @param minDocCount the minimum bucket doc count, or null for base-contract bucket types
     */
    public void setBucketDefinition(BucketOrder order, Integer size, Long minDocCount) {
        addBucketOrder(order);
        this.definingSize = size;
        this.definingMinDocCount = minDocCount;
    }

    /**
     * Requests an implicit COUNT(*) for bucket doc_count.
     * Idempotent — only one COUNT(*) is created at build time.
     */
    public void requestImplicitCount() {
        this.implicitCountRequested = true;
    }

    /** Returns true if this builder has at least one aggregate call or implicit count. */
    public boolean hasAggregateCalls() {
        return !aggregateCalls.isEmpty() || implicitCountRequested;
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
     * Builds the immutable metadata. Resolves grouping indices from the input row type.
     * For no-GROUP-BY metrics, makes return types nullable (AVG of empty set is null).
     *
     * <p>Plan bounds, decided here: {@code min_doc_count} above 1 always becomes a HAVING
     * filter. Root-level sized plans (a single grouping) get a flat LIMIT
     * ({@code fetch = size}); nested sized plans (parent groupings present) get a per-parent
     * bound instead ({@code perParentFetch = size}), enforced by the ROW_NUMBER window plan
     * shape — a flat LIMIT there would keep globally-top groups, not each parent's top K.
     * Multi-field groupings get no bound: the eligible-count machinery (count plans,
     * {@code COUNT(field)}) is single-field today, and an unbounded plan fails loudly at
     * render rather than accounting with a wrong total.
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
            allGroupIndices.addAll(resolveFieldIndices(g, inputRowType));
            allGroupFieldNames.addAll(g.getFieldNames());
        }

        // For no-GROUP-BY, metric results could be null (e.g., AVG of empty set).
        // COUNT stays non-nullable (returns 0).
        boolean noGroupBy = groupings.isEmpty();
        List<AggregateCall> allCalls = new ArrayList<>();
        for (AggregateCall call : aggregateCalls) {
            if (noGroupBy) {
                RelDataType nullableType = typeFactory.createTypeWithNullability(call.getType(), true);
                allCalls.add(
                    AggregateCall.create(
                        call.getAggregation(),
                        call.isDistinct(),
                        call.isApproximate(),
                        call.ignoreNulls(),
                        call.getArgList(),
                        call.filterArg,
                        call.getCollation(),
                        nullableType,
                        call.getName()
                    )
                );
            } else {
                allCalls.add(call);
            }
        }
        List<String> allFieldNames = new ArrayList<>(aggregateFieldNames);

        if (implicitCountRequested) {
            allCalls.add(
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    false,
                    false,
                    false,
                    List.of(),
                    -1,
                    RelCollations.EMPTY,
                    typeFactory.createSqlType(SqlTypeName.BIGINT),
                    IMPLICIT_COUNT_NAME
                )
            );
            allFieldNames.add(IMPLICIT_COUNT_NAME);
        }

        // min_doc_count ≤ 1 needs no HAVING: a GROUP BY group has ≥ 1 row by construction.
        Long havingMinDocCount = definingMinDocCount != null && definingMinDocCount > 1 ? definingMinDocCount : null;
        Integer fetch = null;
        Integer perParentFetch = null;
        boolean singleFieldGroupings = groupings.stream().allMatch(g -> g.getFieldNames().size() == 1);
        if (definingSize != null && singleFieldGroupings) {
            if (groupings.size() == 1) {
                fetch = definingSize;
            } else {
                // Nested level: the bound is per parent, not global — a flat LIMIT would keep
                // globally-top groups, not each parent's top K.
                perParentFetch = definingSize;
            }
        }

        return new AggregationMetadata(
            aggNamePath,
            ImmutableBitSet.of(allGroupIndices),
            allGroupFieldNames,
            allCalls,
            allFieldNames,
            bucketOrders,
            fetch,
            perParentFetch,
            havingMinDocCount,
            missingValues
        );
    }

    /**
     * Resolves field-based grouping names to column indices in the input schema.
     *
     * @param grouping the grouping info containing field names
     * @param inputRowType the schema before aggregation
     * @return column indices for each field name
     * @throws ConversionException if a field name is not found in the schema
     */
    private static List<Integer> resolveFieldIndices(GroupingInfo grouping, RelDataType inputRowType) throws ConversionException {
        List<String> fieldNames = grouping.getFieldNames();
        List<Integer> indices = new ArrayList<>(fieldNames.size());
        for (String name : fieldNames) {
            RelDataTypeField field = inputRowType.getField(name, false, false);
            if (field == null) {
                throw new ConversionException("Group-by field '" + name + "' not found in schema");
            }
            indices.add(field.getIndex());
        }
        return indices;
    }
}
