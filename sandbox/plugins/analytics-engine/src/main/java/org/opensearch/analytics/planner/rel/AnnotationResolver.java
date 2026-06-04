/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.rex.RexNode;

import java.util.function.Function;

/**
 * Extended annotation resolver that performs single-pass bottom-up resolution
 * of a filter condition tree. Classifies each node as delegated or native,
 * and combines same-backend delegated siblings at mixed boundaries.
 *
 * @opensearch.internal
 */
public interface AnnotationResolver extends Function<OperatorAnnotation, RexNode> {

    /**
     * Resolves an entire condition tree in a single bottom-up pass. Combines
     * same-backend delegated siblings at mixed AND/OR boundaries into a single
     * DelegatedExpression. Returns the fully resolved RexNode.
     */
    RexNode resolveTree(RexNode condition);
}
