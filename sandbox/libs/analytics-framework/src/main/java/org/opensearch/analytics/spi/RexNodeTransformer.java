/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

/**
 * Backend-supplied RexNode transformation applied during the marking phase.
 *
 * <p>Each transformer is invoked on every leaf predicate in a filter condition
 * before backend capability matching. The transformer may modify the RexNode
 * (e.g., fold a function call to a literal) or return it unchanged.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface RexNodeTransformer {

    /**
     * Transforms a RexNode, potentially replacing function calls with simplified forms.
     *
     * @param node              the RexNode to transform
     * @param rexBuilder        for creating new RexNodes
     * @param fieldMappingTypes lookup for field mapping types by field index
     * @return the transformed node, or the original if no transformation applies
     */
    RexNode transform(RexNode node, RexBuilder rexBuilder, FieldMappingLookup fieldMappingTypes);

    /**
     * Lookup for field mapping types by field index.
     * Provided by the planner from the child operator's field storage metadata.
     */
    @FunctionalInterface
    interface FieldMappingLookup {
        /** Returns the OpenSearch mapping type for the field at the given index, or null if unavailable. */
        String getMappingType(int fieldIndex);
    }
}
