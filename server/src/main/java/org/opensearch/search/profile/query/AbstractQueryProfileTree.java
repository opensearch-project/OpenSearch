/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.search.Query;
import org.opensearch.search.profile.AbstractInternalProfileTree;
import org.opensearch.search.profile.ContextualProfileBreakdown;
import org.opensearch.search.profile.ProfileResult;

/**
 * This class tracks the dependency tree for queries (scoring and rewriting) and
 * generates {@link QueryProfileBreakdown} for each node in the tree.  It also finalizes the tree
 * and returns a list of {@link ProfileResult} that can be serialized back to the client
 *
 * @opensearch.internal
 */
public abstract class AbstractQueryProfileTree extends AbstractInternalProfileTree<ContextualProfileBreakdown<QueryTimingType>, Query> {

    /** Rewrite time */
    private long rewriteTime;
    private long rewriteScratch;

    @Override
    protected String getTypeFromElement(Query query) {
        // Anonymous classes won't have a name,
        // we need to get the super class
        if (query.getClass().getSimpleName().isEmpty()) {
            return query.getClass().getSuperclass().getSimpleName();
        }
        return query.getClass().getSimpleName();
    }

    @Override
    protected String getDescriptionFromElement(Query query) {
        return query.toString();
    }

    /**
     * Begin timing a query for a specific Timing context
     */
    public void startRewriteTime() {
        assert rewriteScratch == 0;
        rewriteScratch = System.nanoTime();
    }

    /**
     * Halt the timing process and add the elapsed rewriting time.
     * startRewriteTime() must be called for a particular context prior to calling
     * stopAndAddRewriteTime(), otherwise the elapsed time will be negative and
     * nonsensical
     *
     * @return          The elapsed time
     */
    public long stopAndAddRewriteTime() {
        long time = Math.max(1, System.nanoTime() - rewriteScratch);
        rewriteTime += time;
        rewriteScratch = 0;
        return time;
    }

    public long getRewriteTime() {
        return rewriteTime;
    }
}
