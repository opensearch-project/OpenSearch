/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Collector for star tree aggregation
 * This abstract class exposes utilities to help avoid traversing star-tree multiple times and
 * collect relevant metrics across nested aggregations in a single traversal
 * @opensearch.internal
 */
@ExperimentalApi
public abstract class StarTreeBucketCollector {

    protected final StarTreeValues starTreeValues;
    protected final FixedBitSet matchingDocsBitSet;
    protected final List<StarTreeBucketCollector> subCollectors = new ArrayList<>();

    public StarTreeBucketCollector(StarTreeValues starTreeValues, FixedBitSet matchingDocsBitSet) throws IOException {
        this.starTreeValues = starTreeValues;
        this.matchingDocsBitSet = matchingDocsBitSet;
        this.setSubCollectors();
    }

    public StarTreeBucketCollector(StarTreeBucketCollector parent) throws IOException {
        this.starTreeValues = parent.getStarTreeValues();
        this.matchingDocsBitSet = parent.getMatchingDocsBitSet();
        this.setSubCollectors();
    }

    /**
     * Sets the sub-collectors to track nested aggregators
     */
    public void setSubCollectors() throws IOException {};

    /**
     * Returns a list of sub-collectors to track nested aggregators
     */
    public List<StarTreeBucketCollector> getSubCollectors() {
        return subCollectors;
    }

    /**
     * Returns the tree values to iterate
     */
    public StarTreeValues getStarTreeValues() {
        return starTreeValues;
    }

    /**
     * Returns the matching docs bitset to iterate upon the star-tree values based on search query
     */
    public FixedBitSet getMatchingDocsBitSet() {
        return matchingDocsBitSet;
    }

    /**
     * Collects the star tree entry and bucket ordinal to update
     * The method implementation should identify the metrics to collect from that star-tree entry to the specified bucket
     */
    public abstract void collectStarTreeEntry(int starTreeEntry, long bucket) throws IOException;
}
