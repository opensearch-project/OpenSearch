/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * A {@link GroupingInfo} that contributes no GROUP BY fields.
 * Used by filter bucket aggregations, which narrow the document set
 * via a WHERE-style predicate rather than adding grouping columns.
 */
public class EmptyGrouping implements GroupingInfo {

    /** Creates an empty grouping. */
    public EmptyGrouping() {}

    @Override
    public List<String> getFieldNames() {
        return List.of();
    }

    @Override
    public List<Integer> resolveIndices(RelDataType inputRowType) {
        return List.of();
    }
}
