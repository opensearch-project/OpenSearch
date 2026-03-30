/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates monotonically increasing row IDs for cross-format document synchronization.
 * Each writer instance gets its own {@code RowIdGenerator} so that row IDs are unique
 * within a writer's segment scope.
 */
public class RowIdGenerator {

    private final String source;
    private final AtomicLong counter;

    /**
     * Constructs a RowIdGenerator with the given source identifier.
     *
     * @param source a human-readable label identifying the generator's owner (e.g. class name)
     */
    public RowIdGenerator(String source) {
        this.source = source;
        this.counter = new AtomicLong(0);
    }

    /**
     * Returns the next row ID.
     *
     * @return the next monotonically increasing row ID
     */
    public long nextRowId() {
        return counter.getAndIncrement();
    }

    /**
     * Returns the current row ID value without incrementing.
     *
     * @return the current row ID
     */
    public long currentRowId() {
        return counter.get();
    }

    /**
     * Returns the source identifier for this generator.
     *
     * @return the source label
     */
    public String getSource() {
        return source;
    }
}
