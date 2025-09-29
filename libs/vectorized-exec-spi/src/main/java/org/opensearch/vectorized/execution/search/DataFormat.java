/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.search;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 DataFormat supported by OpenSearch
 */
@ExperimentalApi
public enum DataFormat {
    /** CSV Format*/
    CSV("csv"),

    /** Text Format */
    Text("text");

    private final String name;

    DataFormat(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
