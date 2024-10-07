/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Query class for querying star tree data structure.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeQueryContext {

    /**
     * Star tree field info
     * This is used to get the star tree data structure
     */
    private final CompositeIndexFieldInfo starTree;

    /**
     * Map of field name to a value to be queried for that field
     * This is used to filter the data based on the query
     */
    private volatile Map<String, Long> queryMap;

    /**
    * Cache for leaf results
    * This is used to cache the results for each leaf reader context
    * to avoid reading the filtered values from the leaf reader context multiple times
    */
    protected volatile Map<LeafReaderContext, FixedBitSet> starTreeValuesMap;

    public StarTreeQueryContext(CompositeIndexFieldInfo starTree, Map<String, Long> queryMap, boolean cacheStarTreeValues) {
        this.starTree = starTree;
        this.queryMap = queryMap;
        if (cacheStarTreeValues) {
            starTreeValuesMap = new ConcurrentHashMap<>();
        }
    }

    public CompositeIndexFieldInfo getStarTree() {
        return starTree;
    }

    public Map<String, Long> getQueryMap() {
        return queryMap;
    }

    public Map<LeafReaderContext, FixedBitSet> getStarTreeValuesMap() {
        return starTreeValuesMap;
    }
}
