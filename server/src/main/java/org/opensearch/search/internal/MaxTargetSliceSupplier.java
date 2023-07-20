/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Supplier to compute leaf slices based on passed in leaves and max target slice count to limit the number of computed slices. It sorts
 * all the leaves based on document count and then assign each leaf in round-robin fashion to the target slice count slices. Based on
 * experiment results as shared in <a href=https://github.com/opensearch-project/OpenSearch/issues/7358>issue-7358</a>
 * we can see this mechanism helps to achieve better tail/median latency over default lucene slice computation.
 */
public class MaxTargetSliceSupplier {

    public static IndexSearcher.LeafSlice[] getSlices(List<LeafReaderContext> leaves, int target_max_slice) {
        if (target_max_slice <= 0) {
            throw new IllegalArgumentException("MaxTargetSliceSupplier called with unexpected slice count of " + target_max_slice);
        }

        // slice count should not exceed the segment count
        int target_slice_count = Math.min(target_max_slice, leaves.size());

        // Make a copy so we can sort:
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);

        // Sort by maxDoc, descending:
        sortedLeaves.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

        final List<List<LeafReaderContext>> groupedLeaves = new ArrayList<>();
        for (int i = 0; i < target_slice_count; ++i) {
            groupedLeaves.add(new ArrayList<>());
        }
        // distribute the slices in round-robin fashion
        List<LeafReaderContext> group;
        for (int idx = 0; idx < sortedLeaves.size(); ++idx) {
            int currentGroup = idx % target_slice_count;
            group = groupedLeaves.get(currentGroup);
            group.add(sortedLeaves.get(idx));
        }

        IndexSearcher.LeafSlice[] slices = new IndexSearcher.LeafSlice[target_slice_count];
        int upto = 0;
        for (List<LeafReaderContext> currentLeaf : groupedLeaves) {
            slices[upto] = new IndexSearcher.LeafSlice(currentLeaf);
            ++upto;
        }
        return slices;
    }
}
