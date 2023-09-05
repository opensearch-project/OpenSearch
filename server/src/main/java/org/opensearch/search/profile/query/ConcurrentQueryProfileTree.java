/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.opensearch.search.profile.ContextualProfileBreakdown;
import org.opensearch.search.profile.ProfileResult;

import java.util.List;
import java.util.Map;

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

    /**
     * For concurrent query case, when there are nested queries (with children), then the {@link ConcurrentQueryProfileBreakdown} created
     * for the child queries weight doesn't have the association of collector to leaves. This is because child query weights are not
     * exposed by the {@link org.apache.lucene.search.Weight} interface. So after all the collection is happened and before the result
     * tree is created we need to pass the association from parent to the child breakdowns. This will be then used to create the
     * breakdown map at slice level for the child queries as well
     *
     * @return a hierarchical representation of the profiled query tree
     */
    @Override
    public List<ProfileResult> getTree() {
        for (Integer root : roots) {
            final ContextualProfileBreakdown<QueryTimingType> parentBreakdown = breakdowns.get(root);
            assert parentBreakdown instanceof ConcurrentQueryProfileBreakdown;
            final Map<Collector, List<LeafReaderContext>> parentCollectorToLeaves = ((ConcurrentQueryProfileBreakdown) parentBreakdown)
                .getSliceCollectorsToLeaves();
            // update all the children with the parent collectorToLeaves association
            updateCollectorToLeavesForChildBreakdowns(root, parentCollectorToLeaves);
        }
        // once the collector to leaves mapping is updated, get the result
        return super.getTree();
    }

    /**
     * Updates the children with collector to leaves mapping as recorded by parent breakdown
     * @param parentToken parent token number in the tree
     * @param collectorToLeaves collector to leaves mapping recorded by parent
     */
    private void updateCollectorToLeavesForChildBreakdowns(Integer parentToken, Map<Collector, List<LeafReaderContext>> collectorToLeaves) {
        final List<Integer> children = tree.get(parentToken);
        if (children != null) {
            for (Integer currentChild : children) {
                final ContextualProfileBreakdown<QueryTimingType> currentChildBreakdown = breakdowns.get(currentChild);
                currentChildBreakdown.associateCollectorsToLeaves(collectorToLeaves);
                updateCollectorToLeavesForChildBreakdowns(currentChild, collectorToLeaves);
            }
        }
    }
}
