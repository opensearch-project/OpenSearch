/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;

/**
 * Util class for building star tree
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeBuilderUtils {

    private StarTreeBuilderUtils() {}

    public static final int ALL = -1;

    /** Tree node representation */
    public static class TreeNode {
        public int dimensionId = ALL;
        public int startDocId = ALL;
        public int endDocId = ALL;
        public int aggregatedDocId = ALL;
        public int childDimensionId = ALL;
        public long dimensionValue = ALL;
        public boolean isStarNode = false;
        public Map<Long, TreeNode> children;
    }

}
