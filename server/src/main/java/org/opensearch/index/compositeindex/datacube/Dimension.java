/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContent;

import java.util.List;

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
     * Sets the dimension values in the provided array starting from the given index.
     *
     * @param value   The value to be set
     * @param dims  The dimensions array to set the values in
     * @param index The starting index in the array
     * @return The next available index in the array
     */
    int setDimensionValues(Long value, Long[] dims, int index);

    /**
     * Returns the list of dimension fields that represent the dimension
     */
    List<String> getDimensionFieldsNames();
}
