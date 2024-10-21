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
    private final Map<String, Long> queryMap;

    /**
    * Cache for leaf results
    * This is used to cache the results for each leaf reader context
    * to avoid reading the filtered values from the leaf reader context multiple times
    */
    private final FixedBitSet[] starTreeValues;

    public StarTreeQueryContext(CompositeIndexFieldInfo starTree, Map<String, Long> queryMap, int numSegmentsCache) {
        this.starTree = starTree;
        this.queryMap = queryMap;
        if (numSegmentsCache > -1) {
            starTreeValues = new FixedBitSet[numSegmentsCache];
        } else {
            starTreeValues = null;
        }
    }

    public CompositeIndexFieldInfo getStarTree() {
        return starTree;
    }

    public Map<String, Long> getQueryMap() {
        return queryMap;
    }

    public FixedBitSet[] getStarTreeValues() {
        return starTreeValues;
    }

    public FixedBitSet getStarTreeValues(LeafReaderContext ctx) {
        if (starTreeValues != null) {
            return starTreeValues[ctx.ord];
        }
        return null;
    }

    public void setStarTreeValues(LeafReaderContext ctx, FixedBitSet values) {
        if (starTreeValues != null) {
            starTreeValues[ctx.ord] = values;
        }
    }
}
