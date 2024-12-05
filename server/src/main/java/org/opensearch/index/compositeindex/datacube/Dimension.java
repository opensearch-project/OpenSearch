/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.apache.lucene.index.DocValuesType;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContent;

import java.util.List;
import java.util.function.Consumer;

/**
 * Base interface for data-cube dimensions
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface Dimension extends ToXContent {

    String getField();

    /**
     * Returns the number of dimension values that gets added to star tree document
     * as part of this dimension
     */
    int getNumSubDimensions();

    /**
     * Sets the dimension values with the consumer
     *
     * @param value   The value to be set
     * @param dimSetter  Consumer which sets the dimensions
     */
    void setDimensionValues(final Long value, final Consumer<Long> dimSetter);

    /**
     * Returns the list of dimension fields that represent the dimension
     */
    List<String> getSubDimensionNames();

    DocValuesType getDocValuesType();
}
