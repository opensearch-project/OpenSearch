/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils.date;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Interface for rounding time units in starTree
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DateTimeUnitRounding {
    /**
     * Returns the short name of the time unit
     */
    String shortName();

    /**
     * rounds down the given utcMillis to the nearest unit of time
     */
    long roundFloor(long utcMillis);
}
