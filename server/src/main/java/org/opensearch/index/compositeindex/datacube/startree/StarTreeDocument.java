/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Arrays;

/**
 * Star tree document
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeDocument {
    public final Long[] dimensions;
    public final Object[] metrics;

    public StarTreeDocument(Long[] dimensions, Object[] metrics) {
        this.dimensions = dimensions;
        this.metrics = metrics;
    }

    @Override
    public String toString() {
        return Arrays.toString(dimensions) + " | " + Arrays.toString(metrics);
    }
}
