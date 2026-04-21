/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.opensearch.analytics.spi.ExchangeSink;

/**
 * Read-only interface for consuming accumulated results from a stage exchange.
 * Consumers (parent stages, the walker's completion listener) read from this;
 * they never write to it.
 *
 * @see ExchangeSink for the write-side counterpart
 */
public interface ExchangeSource {

    /**
     * Return all accumulated rows in insertion order.
     * Converts columnar Arrow batches to row-oriented {@code Object[]} arrays.
     */
    Iterable<Object[]> readResult();

    /**
     * Return the total number of accumulated rows across all batches.
     */
    long getRowCount();
}
