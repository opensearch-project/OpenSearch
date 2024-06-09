/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.startree.builder;

import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.index.compositeindex.startree.node.StarTree;

import java.util.Map;

/**
 * Star tree aggregated values holder for reader / query
 * @opensearch.experimental
 */
public class StarTreeDocValues {
    public StarTree starTree;

    // Based on the implementation, these NEED to be INORDER or implementation of LinkedHashMap
    // We use SortedNumericDocValues because essentially everything is stored as long
    public Map<String, SortedNumericDocValues> dimensionValues;
    public Map<String, SortedNumericDocValues> metricValues;

    public StarTreeDocValues(
        StarTree starTree,
        Map<String, SortedNumericDocValues> dimensionValues,
        Map<String, SortedNumericDocValues> metricValues
    ) {
        this.starTree = starTree;
        this.dimensionValues = dimensionValues;
        this.metricValues = metricValues;
    }
}
