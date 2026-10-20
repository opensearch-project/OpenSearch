/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.opensearch.dsl.aggregation.AggregationMetadata;

/**
 * Creates a {@link LogicalAggregate} from pre-computed {@link AggregationMetadata}.
 * The metadata is produced by the tree walker and set on the context before this runs.
 */
public class AggregateConverter {

    /** Creates an aggregate converter. */
    public AggregateConverter() {}

    /**
     * Builds a LogicalAggregate from the given metadata.
     *
     * @param input the input plan (scan + filter)
     * @param metadata pre-computed aggregation metadata for one granularity
     * @return the LogicalAggregate node
     */
    public RelNode convert(RelNode input, AggregationMetadata metadata) {
        return LogicalAggregate.create(input, metadata.getGroupByBitSet(), null, metadata.getAggregateCalls());
    }
}
