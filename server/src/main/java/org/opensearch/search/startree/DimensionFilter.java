/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;

import java.io.IOException;
import java.util.List;

public interface DimensionFilter {

    public void initialiseForSegment(LeafReaderContext leafReaderContext, StarTreeValues starTreeValues) throws IOException;

    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, List<StarTreeNode> collector) throws IOException;

    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues);

}
