/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Composite index dimension base class
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Dimension {
    private final String field;

    public Dimension(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    public void setDefaults(CompositeIndexSettings compositeIndexSettings) {
        // no implementation
    }
}
