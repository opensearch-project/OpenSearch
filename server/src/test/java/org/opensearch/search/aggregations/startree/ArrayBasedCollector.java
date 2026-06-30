/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.search.startree.StarTreeNodeCollector;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ArrayBasedCollector implements StarTreeNodeCollector {

    private final Set<Long> nodeDimensionValues = new HashSet<>();

    @Override
    public void collectStarTreeNode(StarTreeNode node) {
        try {
            nodeDimensionValues.add(node.getDimensionValue());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean matchAllCollectedValues(long... values) throws IOException {
        for (long value : values) {
            if (!nodeDimensionValues.contains(value)) return false;
        }
        return true;
    }

    public int collectedNodeCount() {
        return nodeDimensionValues.size();
    }

}
