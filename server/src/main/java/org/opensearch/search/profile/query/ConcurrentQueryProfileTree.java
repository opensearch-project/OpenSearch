/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.opensearch.search.profile.ContextualProfileBreakdown;
import org.opensearch.search.profile.ProfileResult;

import java.util.List;

/**
 * This class returns a list of {@link ProfileResult} that can be serialized back to the client in the concurrent execution.
 *
 * @opensearch.internal
 */
public class ConcurrentQueryProfileTree extends AbstractQueryProfileTree {

    @Override
    protected ContextualProfileBreakdown<QueryTimingType> createProfileBreakdown() {
        return new ConcurrentQueryProfileBreakdown();
    }

    @Override
    protected ProfileResult createProfileResult(
        String type,
        String description,
        ContextualProfileBreakdown<QueryTimingType> breakdown,
        List<ProfileResult> childrenProfileResults
    ) {
        assert breakdown instanceof ConcurrentQueryProfileBreakdown;
        final ConcurrentQueryProfileBreakdown concurrentBreakdown = (ConcurrentQueryProfileBreakdown) breakdown;
        return new ProfileResult(
            type,
            description,
            concurrentBreakdown.toBreakdownMap(),
            concurrentBreakdown.toDebugMap(),
            concurrentBreakdown.toNodeTime(),
            childrenProfileResults,
            concurrentBreakdown.getMaxSliceNodeTime(),
            concurrentBreakdown.getMinSliceNodeTime(),
            concurrentBreakdown.getAvgSliceNodeTime()
        );
    }
}
