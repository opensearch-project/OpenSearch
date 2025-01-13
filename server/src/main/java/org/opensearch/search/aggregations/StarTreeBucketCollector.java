/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class StarTreeBucketCollector {

    protected StarTreeValues starTreeValues;
    protected FixedBitSet matchingDocsBitSet;
    protected final List<StarTreeBucketCollector> subCollectors = new ArrayList<>();

    public List<StarTreeBucketCollector> getSubCollectors() {
        return subCollectors;
    }

    public FixedBitSet getMatchingDocsBitSet() {
        return matchingDocsBitSet;
    }

    public abstract void collectStarTreeEntry(int starTreeEntry, long bucket) throws IOException;
}
