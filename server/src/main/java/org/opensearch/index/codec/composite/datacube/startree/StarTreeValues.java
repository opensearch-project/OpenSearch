/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.datacube.startree;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.CompositeIndexValues;

import java.util.List;

/**
 * Concrete class that holds the star tree associated values from the segment
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeValues implements CompositeIndexValues {
    private final List<String> dimensionsOrder;

    // TODO : come up with full set of vales such as dimensions and metrics doc values + star tree
    public StarTreeValues(List<String> dimensionsOrder) {
        super();
        this.dimensionsOrder = List.copyOf(dimensionsOrder);
    }

    @Override
    public CompositeIndexValues getValues() {
        return this;
    }
}
