/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.search.spi;

import java.util.List;
import java.util.Map;

/**
 * Service Provider Interface for query execution results.
 * Implementations provide access to columnar query results from different execution engines.
 * 
 * @opensearch.experimental
 */
public interface QueryResult {

    /**
     * Returns the columnar result data where each entry maps a column name to its list of values.
     * 
     * @return Map of column names to their corresponding value lists
     */
    Map<String, List<Object>> getColumns();
}
