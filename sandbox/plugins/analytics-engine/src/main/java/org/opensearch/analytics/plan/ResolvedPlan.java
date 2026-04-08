/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan;

import org.apache.calcite.rel.RelNode;

/**
 * An immutable value type representing a fully resolved query plan,
 * consisting of the optimized and backend-tagged {@link RelNode} tree
 * and the name of the backend that will execute it.
 *
 * <p>Produced by {@link QueryPlanner#plan(RelNode, int)} after all five
 * planning phases (validate → optimize → wrap → agg-split → resolve)
 * have completed successfully.
 */
public final class ResolvedPlan {

    private final RelNode root;
    private final String backendName;

    /**
     * Constructs a resolved plan.
     *
     * @param root        the root of the resolved, backend-tagged operator tree
     * @param backendName the name of the backend that will execute this plan
     */
    public ResolvedPlan(RelNode root, String backendName) {
        this.root = root;
        this.backendName = backendName;
    }

    /**
     * Returns the root of the resolved operator tree.
     *
     * @return the root {@link RelNode}
     */
    public RelNode getRoot() {
        return root;
    }

    /**
     * Returns the name of the backend assigned to execute this plan.
     *
     * @return the backend name (e.g. {@code "datafusion"}, {@code "lucene"})
     */
    public String getBackendName() {
        return backendName;
    }
}
