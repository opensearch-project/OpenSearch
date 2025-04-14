/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link HybridQueryRewriteCollectorManager} is responsible for creating {@link HybridQueryExecutorCollector}
 * instances. Useful to create {@link HybridQueryExecutorCollector} instances that rewrites {@link Query} into primitive
 * {@link Query} using {@link IndexSearcher}
 */
public final class HybridQueryRewriteCollectorManager implements HybridQueryExecutorCollectorManager<HybridQueryExecutorCollector> {

    private final IndexSearcher searcher;

    public HybridQueryRewriteCollectorManager(IndexSearcher searcher) {
        if (searcher == null) {
            throw new NullPointerException("searcher is marked non-null but is null");
        }
        this.searcher = searcher;
    };

    /**
     * Returns new {@link HybridQueryExecutorCollector} to facilitate parallel execution
     * @return HybridQueryExecutorCollector instance
     */
    @Override
    public HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>> newCollector() {
        return HybridQueryExecutorCollector.newCollector(searcher);
    }

    /**
     * Returns list of {@link Query} that were rewritten by collectors. If collector doesn't
     * have any result, null will be inserted to the result.
     * This method must be called after collection is finished on all provided collectors.
     * @param collectors list of collectors
     * @return list of {@link Query} that was rewritten by corresponding collector from input.
     */
    public List<Query> getQueriesAfterRewrite(List<HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>>> collectors) {
        List<Query> rewrittenQueries = new ArrayList<>();
        for (HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>> collector : collectors) {
            if (collector.getResult().isPresent()) {
                rewrittenQueries.add(collector.getResult().get().getKey());
            } else {
                // if for some reason collector didn't have result, we will add null to its
                // position in the result.
                rewrittenQueries.add(null);
            }
        }
        return rewrittenQueries;
    }

    /**
     * Returns true if any of the {@link Query} from collector were actually rewritten.
     * If any of the given collector doesn't have result, it will be ignored as if that
     * instance did not exist. This method must be called after collection is finished
     * on all provided collectors.
     * @param collectors List of collectors to check any of their query was rewritten during
     *                   collect step.
     * @return at least one query is rewritten by any of the collectors
     */
    public boolean anyQueryRewrite(List<HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>>> collectors) {
        // return true if at least one query is rewritten
        for (HybridQueryExecutorCollector<IndexSearcher, Map.Entry<Query, Boolean>> collector : collectors) {
            final Optional<Map.Entry<Query, Boolean>> result = collector.getResult();
            if (result.isPresent() && result.get().getValue()) {
                return true;
            }
        }
        return false;
    }
}
