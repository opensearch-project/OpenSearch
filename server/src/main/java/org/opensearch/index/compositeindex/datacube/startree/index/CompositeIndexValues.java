/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.index;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Interface for composite index values
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CompositeIndexValues {
    CompositeIndexValues getValues();
}
