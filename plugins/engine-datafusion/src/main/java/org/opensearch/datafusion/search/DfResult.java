/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.opensearch.vectorized.execution.search.spi.QueryResult;

import java.util.List;
import java.util.Map;

/**
 * Wraps the columnar result from a DataFusion query execution.
 * Each entry maps a column name to its list of values.
 * Implements the QueryResult SPI to allow usage in core without creating a dependency.
 */
public class DfResult implements QueryResult {

    private final Map<String, List<Object>> columns;

    public DfResult(Map<String, List<Object>> columns) {
        this.columns = columns;
    }

    @Override
    public Map<String, List<Object>> getColumns() {
        return columns;
    }
}
