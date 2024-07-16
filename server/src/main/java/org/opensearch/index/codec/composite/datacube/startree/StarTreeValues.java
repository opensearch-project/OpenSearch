/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.datacube.startree;

import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.CompositeIndexValues;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;

import java.util.Map;

/**
 * Concrete class that holds the star tree associated values from the segment
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeValues implements CompositeIndexValues {
    private final StarTreeField starTreeField;
    private final StarTreeNode root;
    private final Map<String, DocIdSetIterator> dimensionDocValuesIteratorMap;
    private final Map<String, DocIdSetIterator> metricDocValuesIteratorMap;

    public StarTreeValues(
        StarTreeField starTreeField,
        StarTreeNode root,
        Map<String, DocIdSetIterator> dimensionDocValuesIteratorMap,
        Map<String, DocIdSetIterator> metricDocValuesIteratorMap
    ) {
        this.starTreeField = starTreeField;
        this.root = root;
        this.dimensionDocValuesIteratorMap = dimensionDocValuesIteratorMap;
        this.metricDocValuesIteratorMap = metricDocValuesIteratorMap;
    }

    @Override
    public CompositeIndexValues getValues() {
        return this;
    }

    public StarTreeField getStarTreeField() {
        return starTreeField;
    }

    public StarTreeNode getRoot() {
        return root;
    }

    public Map<String, DocIdSetIterator> getDimensionDocValuesIteratorMap() {
        return dimensionDocValuesIteratorMap;
    }

    public Map<String, DocIdSetIterator> getMetricDocValuesIteratorMap() {
        return metricDocValuesIteratorMap;
    }
}
