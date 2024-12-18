/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.fileformats.meta;

import org.apache.lucene.index.DocValuesType;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.ComparatorType;

@ExperimentalApi
public class DimensionProperty {

    private final DocValuesType docValuesType;
    private final ComparatorType comparatorType;

    public DimensionProperty(DocValuesType docValuesType, ComparatorType comparatorType) {
        this.docValuesType = docValuesType;
        this.comparatorType = comparatorType;
    }

    public DocValuesType getDocValuesType() {
        return docValuesType;
    }

    public ComparatorType getComparatorType() {
        return comparatorType;
    }
}
