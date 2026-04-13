/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.dsl.aggregation.AggregationMetadata;
import org.opensearch.dsl.aggregation.AggregationMetadataBuilder;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalOrder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves {@link BucketOrder} objects to Calcite {@link RelFieldCollation} using
 * the actual post-aggregation schema from the {@code LogicalAggregate} node.
 *
 * All field lookups are name-based against the real {@link RelDataType} —
 * zero positional assumptions about the output schema layout.
 */
public final class CollationResolver {

    private static final String KEY_FIELD = "_key";

    private CollationResolver() {}

    /**
     * Resolves bucket orders to field collations using the actual post-agg schema.
     *
     * @param metadata The aggregation metadata containing bucket orders and field name lists
     * @param postAggRowType The actual row type of the LogicalAggregate node
     * @return List of field collations for LogicalSort
     * @throws ConversionException if a sort field cannot be resolved
     */
    public static List<RelFieldCollation> resolve(
            AggregationMetadata metadata, RelDataType postAggRowType) throws ConversionException {

        Map<String, List<Integer>> postAggFieldIndex = buildPostAggFieldIndex(metadata, postAggRowType);

        List<RelFieldCollation> collations = new ArrayList<>();
        for (BucketOrder order : metadata.getBucketOrders()) {
            resolveOrder(order, postAggFieldIndex, collations);
        }
        return collations;
    }

    /**
     * Parses a single BucketOrder and appends the corresponding RelFieldCollation(s).
     */
    private static void resolveOrder(BucketOrder order, Map<String, List<Integer>> postAggFieldIndex,
            List<RelFieldCollation> collations) throws ConversionException {

        String fieldName;
        boolean ascending;

        if (order instanceof InternalOrder.Aggregation aggOrder) {
            fieldName = aggOrder.path().toString();
            ascending = aggOrder.equals(BucketOrder.aggregation(fieldName, true));
        } else if (InternalOrder.isKeyOrder(order)) {
            fieldName = "_key";
            ascending = InternalOrder.isKeyAsc(order);
        } else if (InternalOrder.isCountDesc(order)) {
            fieldName = "_count";
            ascending = false;
        } else if (order.equals(BucketOrder.count(true))) {
            fieldName = "_count";
            ascending = true;
        } else {
            throw new ConversionException(
                "Unsupported BucketOrder type: " + order.getClass().getName());
        }

        List<Integer> indices = postAggFieldIndex.get(fieldName);
        if (indices == null) {
            throw new ConversionException(
                "Sort field '" + fieldName + "' not found. Available: " + postAggFieldIndex.keySet());
        }

        RelFieldCollation.Direction direction = ascending
            ? RelFieldCollation.Direction.ASCENDING
            : RelFieldCollation.Direction.DESCENDING;
        RelFieldCollation.NullDirection nullDirection = ascending
            ? RelFieldCollation.NullDirection.LAST
            : RelFieldCollation.NullDirection.FIRST;

        for (int idx : indices) {
            collations.add(new RelFieldCollation(idx, direction, nullDirection));
        }
    }

    /**
     * Builds the sort field map using only name-based lookup against the actual schema.
     */
    private static Map<String, List<Integer>> buildPostAggFieldIndex(
            AggregationMetadata metadata, RelDataType postAggRowType) {

        Map<String, List<Integer>> map = new HashMap<>();
        List<RelDataTypeField> fields = postAggRowType.getFieldList();

        Map<String, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            nameToIndex.put(fields.get(i).getName(), i);
        }

        List<String> groupByFields = metadata.getGroupByFieldNames();
        List<Integer> keyIndices = new ArrayList<>(groupByFields.size());
        for (String columnName : groupByFields) {
            Integer idx = nameToIndex.get(columnName);
            if (idx != null) {
                keyIndices.add(idx);
                map.put(columnName, List.of(idx));
            }
        }
        map.put(KEY_FIELD, keyIndices);

        for (String aggName : metadata.getAggregateFieldNames()) {
            Integer idx = nameToIndex.get(aggName);
            if (idx != null) {
                map.put(aggName, List.of(idx));
            }
        }

        Integer countIdx = nameToIndex.get(AggregationMetadataBuilder.IMPLICIT_COUNT_NAME);
        if (countIdx != null) {
            map.put(AggregationMetadataBuilder.IMPLICIT_COUNT_NAME, List.of(countIdx));
        }

        return map;
    }
}
