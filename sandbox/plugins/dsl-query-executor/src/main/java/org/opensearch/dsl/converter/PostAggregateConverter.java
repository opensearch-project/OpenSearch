/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;

import java.util.List;

/**
 * Applies post-aggregation sorting from BucketOrder collations.
 *
 * Uses {@link CollationResolver} to resolve bucket orders against the actual
 * post-aggregation schema from the LogicalAggregate node.
 */
public class PostAggregateConverter extends AbstractDslConverter {

    /** Creates a post-aggregate converter. */
    public PostAggregateConverter() {}

    @Override
    protected boolean isApplicable(ConversionContext ctx) {
        return ctx.getAggregationMetadata() != null && ctx.getAggregationMetadata().hasBucketOrders();
    }

    @Override
    protected RelNode doConvert(RelNode input, ConversionContext ctx) throws ConversionException {
        List<RelFieldCollation> collations = CollationResolver.resolve(ctx.getAggregationMetadata(), input.getRowType());
        if (collations.isEmpty()) {
            return input;
        }
        RelCollation relCollation = RelCollations.of(collations);
        return LogicalSort.create(input, relCollation, null, null);
    }
}
