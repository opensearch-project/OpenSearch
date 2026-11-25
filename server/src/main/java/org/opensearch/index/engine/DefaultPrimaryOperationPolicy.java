/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import java.io.IOException;

/**
 * The standard primary indexing policy: the primary generates its own sequence numbers, plans
 * operations with full primary semantics, and fills sequence-number gaps on promotion.
 * <p>
 * This is the provider used when no plugin supplies one, and it reproduces the behavior
 * {@link InternalEngine} had before {@link PrimaryOperationPolicy} existed.
 *
 */
public final class DefaultPrimaryOperationPolicy implements PrimaryOperationPolicy {

    /** Shared stateless instance. */
    public static final DefaultPrimaryOperationPolicy INSTANCE = new DefaultPrimaryOperationPolicy();

    private DefaultPrimaryOperationPolicy() {}

    @Override
    public boolean acceptsPreAssignedSeqNos() {
        return false;
    }

    @Override
    public IndexingStrategy planIndex(OperationStrategyPlanner<Engine.Index, IndexingStrategy> planner, Engine.Index index)
        throws IOException {
        return planner.planOperationAsPrimary(index);
    }

    @Override
    public DeletionStrategy planDelete(OperationStrategyPlanner<Engine.Delete, DeletionStrategy> planner, Engine.Delete delete)
        throws IOException {
        return planner.planOperationAsPrimary(delete);
    }

    @Override
    public String toString() {
        return "default";
    }
}
