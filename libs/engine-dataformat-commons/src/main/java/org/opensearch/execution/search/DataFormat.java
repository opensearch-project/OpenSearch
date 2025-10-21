/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.execution.search;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;
import java.util.Map;

/**
 * Represents a data format.
 */
@ExperimentalApi
public interface DataFormat {

    /**
     *
     * @return name identifier for the data format.
     */
    String name();
}
