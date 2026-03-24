/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.Map;

/**
 * An immutable value type representing a fully resolved query plan,
 * consisting of the optimized and backend-tagged {@link RelNode} tree,
 * the name of the backend that will execute it, and any delegation
 * predicates that secondary backends must evaluate.
 */
public final class ResolvedPlan {

    private final RelNode root;
    private final String primaryBackend;
    private final Map<String, RexNode> delegationPredicates;

    public ResolvedPlan(RelNode root, String primaryBackend, Map<String, RexNode> delegationPredicates) {
        this.root = root;
        this.primaryBackend = primaryBackend;
        this.delegationPredicates = Map.copyOf(delegationPredicates);
    }

    public RelNode getRoot() {
        return root;
    }

    public String getPrimaryBackend() {
        return primaryBackend;
    }

    /** Predicates delegated to secondary backends (backend name → predicate). Empty if no delegation. */
    public Map<String, RexNode> getDelegationPredicates() {
        return delegationPredicates;
    }
}
