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

    private CollationResolver() {}

    /**
     * Resolves bucket orders to field collations using the actual post-agg schema.
     *
     * @param metadata the aggregation metadata containing bucket orders and field name lists
     * @param postAggRowType the actual row type of the LogicalAggregate node
     * @return list of field collations for LogicalSort
     * @throws ConversionException if a sort field cannot be resolved
     */
    public static List<RelFieldCollation> resolve(AggregationMetadata metadata, RelDataType postAggRowType) throws ConversionException {
        Map<String, List<Integer>> postAggFieldIndex = buildPostAggFieldIndex(metadata, postAggRowType);

        List<RelFieldCollation> collations = new ArrayList<>();
        for (BucketOrder order : metadata.getBucketOrders()) {
            resolveOrder(order, postAggFieldIndex, collations);
        }
        return collations;
    }

    /** Resolved field name and sort direction from a BucketOrder. */
    private record OrderTarget(String fieldName, boolean ascending) {
    }

    /**
     * Maps a single BucketOrder to one or more RelFieldCollations.
     * CompoundOrders are never seen here — they are flattened by AggregationMetadataBuilder.addBucketOrder.
     */
    private static void resolveOrder(BucketOrder order, Map<String, List<Integer>> postAggFieldIndex, List<RelFieldCollation> collations)
        throws ConversionException {
        // Determine which field to sort on and in which direction
        OrderTarget target = resolveOrderTarget(order);

        // Look up the field's post-agg schema index(es); _key may resolve to multiple indices
        List<Integer> indices = postAggFieldIndex.get(target.fieldName);
        if (indices == null) {
            throw new ConversionException("Sort field '" + target.fieldName + "' not found. Available: " + postAggFieldIndex.keySet());
        }

        // Map direction: ASC nulls last, DESC nulls first (matches OpenSearch default)
        RelFieldCollation.Direction direction = target.ascending
            ? RelFieldCollation.Direction.ASCENDING
            : RelFieldCollation.Direction.DESCENDING;
        RelFieldCollation.NullDirection nullDirection = target.ascending
            ? RelFieldCollation.NullDirection.LAST
            : RelFieldCollation.NullDirection.FIRST;

        // Create a collation per index (one for most fields, multiple for _key with nested GROUP BY)
        for (int idx : indices) {
            collations.add(new RelFieldCollation(idx, direction, nullDirection));
        }
    }

    private static OrderTarget resolveOrderTarget(BucketOrder order) throws ConversionException {
        if (order instanceof InternalOrder.Aggregation aggOrder) {
            return resolveMetricOrder(aggOrder);
        } else if (InternalOrder.isKeyOrder(order)) {
            return resolveKeyOrder(order);
        } else if (InternalOrder.isCountDesc(order) || order.equals(BucketOrder.count(true))) {
            return resolveCountOrder(order);
        }
        throw new ConversionException("Unsupported BucketOrder type: " + order.getClass().getName());
    }

    // InternalOrder.Aggregation.order is private with no accessor; equality check is the only way
    private static OrderTarget resolveMetricOrder(InternalOrder.Aggregation aggOrder) {
        String fieldName = aggOrder.path().toString();
        boolean ascending = aggOrder.equals(BucketOrder.aggregation(fieldName, true));
        return new OrderTarget(fieldName, ascending);
    }

    private static OrderTarget resolveKeyOrder(BucketOrder order) {
        return new OrderTarget("_key", InternalOrder.isKeyAsc(order));
    }

    // no InternalOrder.isCountAsc() helper; count desc is checked via isCountDesc(), count asc via equality
    private static OrderTarget resolveCountOrder(BucketOrder order) {
        return new OrderTarget("_count", !InternalOrder.isCountDesc(order));
    }

    /**
     * Builds a map from sort field names to their indices in the post-aggregation schema.
     *
     * <p>Post-agg schema ordering: GROUP BY fields first (ordered by ImmutableBitSet, i.e. input
     * column index), then aggregate calls in the order they were added to the builder.
     *
     * <p>_key indices follow the tree walker's accumulation order (outer bucket first), which may
     * differ from the schema position order. For example, terms(brand) → terms(name) produces
     * schema [name(0), brand(1), ...] but _key indices [brand(1), name(0)] (outer first).
     */
    private static Map<String, List<Integer>> buildPostAggFieldIndex(AggregationMetadata metadata, RelDataType postAggRowType) {
        Map<String, List<Integer>> map = new HashMap<>();
        List<RelDataTypeField> fields = postAggRowType.getFieldList();

        Map<String, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            nameToIndex.put(fields.get(i).getName(), i);
        }

        // _key → each GROUP BY field in accumulation order (outer bucket first)
        List<Integer> keyIndices = new ArrayList<>();
        for (String groupByName : metadata.getGroupByFieldNames()) {
            Integer idx = nameToIndex.get(groupByName);
            if (idx != null) {
                keyIndices.add(idx);
                map.put(groupByName, List.of(idx));
            }
        }
        map.put("_key", keyIndices);

        // Metric fields
        for (String aggName : metadata.getAggregateFieldNames()) {
            Integer idx = nameToIndex.get(aggName);
            if (idx != null) {
                map.put(aggName, List.of(idx));
            }
        }

        // _count
        Integer countIdx = nameToIndex.get(AggregationMetadataBuilder.IMPLICIT_COUNT_NAME);
        if (countIdx != null) {
            map.put(AggregationMetadataBuilder.IMPLICIT_COUNT_NAME, List.of(countIdx));
        }

        return map;
    }
}
