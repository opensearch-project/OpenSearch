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
}
