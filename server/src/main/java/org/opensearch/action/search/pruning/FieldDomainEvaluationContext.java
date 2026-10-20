/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Request-scoped context used while evaluating field domains against query constraints.
 */
public final class FieldDomainEvaluationContext {
    private final LongSupplier nowInMillisSupplier;

    /**
     * Creates an evaluation context.
     *
     * @param nowInMillisSupplier supplier for the search request's absolute start time in epoch milliseconds
     */
    public FieldDomainEvaluationContext(LongSupplier nowInMillisSupplier) {
        this.nowInMillisSupplier = Objects.requireNonNull(nowInMillisSupplier, "nowInMillisSupplier must not be null");
    }

    /**
     * Supplier for resolving date math expressions such as {@code now}.
     */
    public LongSupplier nowInMillisSupplier() {
        return nowInMillisSupplier;
    }

    /**
     * Resolves the request's absolute start time in epoch milliseconds.
     */
    public long nowInMillis() {
        return nowInMillisSupplier.getAsLong();
    }
}
