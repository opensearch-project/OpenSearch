/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.search.startree.StarTreeNodeCollector;

import java.io.IOException;

@ExperimentalApi
public interface DimensionFilter {

    public void initialiseForSegment(StarTreeValues starTreeValues) throws IOException;

    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
        throws IOException;

    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues);

}
