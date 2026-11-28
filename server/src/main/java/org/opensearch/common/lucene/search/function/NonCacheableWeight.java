/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;

/**
 * Delegates matching to the wrapped weight while reporting itself as non-cacheable.
 *
 * <p>Used on the no-scores path of {@link FunctionScoreQuery} and {@link ScriptScoreQuery}, which return the
 * sub-query's weight there. The score-bearing query object still stays as a clause of any parent that holds it, so a
 * cached parent would keep that query, its score script, and the request's searcher reachable
 * (opensearch-project/OpenSearch#23598). Reporting non-cacheability stops such a parent from being cached.
 *
 * @opensearch.internal
 */
final class NonCacheableWeight extends FilterWeight {

    NonCacheableWeight(Query query, Weight in) {
        super(query, in);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }
}
