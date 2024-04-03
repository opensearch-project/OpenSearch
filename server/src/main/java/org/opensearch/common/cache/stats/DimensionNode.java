/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import java.util.Map;

/**
 * A node in a tree structure, which stores stats in StatsHolder or CacheStats implementations.
 */
abstract class DimensionNode {
    private final String dimensionValue;

    DimensionNode(String dimensionValue) {
        this.dimensionValue = dimensionValue;
    }

    public String getDimensionValue() {
        return dimensionValue;
    }

    protected abstract void createChildrenMap();

    protected abstract Map<String, ? extends DimensionNode> getChildren();

    public boolean hasChildren() {
        return getChildren() != null && !getChildren().isEmpty();
    }
}
