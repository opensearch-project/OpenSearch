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
import java.util.ArrayList;
import java.util.List;

public class ArrayBasedCollector implements StarTreeNodeCollector {

    private final List<StarTreeNode> nodes = new ArrayList<>();

    @Override
    public void collectStarTreeNode(StarTreeNode node) {
        nodes.add(node);
    }

    public boolean matchValues(long[] values) throws IOException {
        boolean matches = true;
        for (int i = 0; i < values.length; i++) {
            matches &= nodes.get(i).getDimensionValue() == values[i];
        }
        return matches;
    }

    public int collectedNodeCount() {
        return nodes.size();
    }

}
