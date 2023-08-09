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
 *
 * @opensearch.internal
 */
final class MaxTargetSliceSupplier {

    static IndexSearcher.LeafSlice[] getSlices(List<LeafReaderContext> leaves, int targetMaxSlice) {
        if (targetMaxSlice <= 0) {
            throw new IllegalArgumentException("MaxTargetSliceSupplier called with unexpected slice count of " + targetMaxSlice);
        }

        // slice count should not exceed the segment count
        int targetSliceCount = Math.min(targetMaxSlice, leaves.size());

        // Make a copy so we can sort:
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);

        // Sort by maxDoc, descending:
        sortedLeaves.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

        final List<List<LeafReaderContext>> groupedLeaves = new ArrayList<>(targetSliceCount);
        for (int i = 0; i < targetSliceCount; ++i) {
            groupedLeaves.add(new ArrayList<>());
        }
        // distribute the slices in round-robin fashion
        for (int idx = 0; idx < sortedLeaves.size(); ++idx) {
            int currentGroup = idx % targetSliceCount;
            groupedLeaves.get(currentGroup).add(sortedLeaves.get(idx));
        }

        return groupedLeaves.stream().map(IndexSearcher.LeafSlice::new).toArray(IndexSearcher.LeafSlice[]::new);
    }
}
