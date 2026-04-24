/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import java.util.List;

/**
 * A QueryBuilder which can provide QueryBuilders that make up the complement of the original query.
 */
public interface ComplementAwareQueryBuilder {
    /**
     * Returns a list of RangeQueryBuilder whose elements, when combined, form the complement of this range query.
     * May be null, in which case the complement couldn't be determined.
     * @return the complement
     */
    List<? extends QueryBuilder> getComplement(QueryShardContext context);

    /**
     * Returns whether the rewriter must verify that all documents have exactly one value for
     * the field before applying the complement rewrite.
     * <p>
     * Most complement rewrites (e.g. range, term) are only valid when the field is single-valued
     * and present in every document. Implementations where the complement semantics are independent
     * of field cardinality (e.g. {@code exists}/{@code not_exists}) should override this to return
     * {@code false} to bypass the check.
     *
     * @return {@code true} if the cardinality check must pass before rewriting (default); {@code false} to skip it
     */
    default boolean requiresCardinalityCheck() {
        return true;
    }
}
