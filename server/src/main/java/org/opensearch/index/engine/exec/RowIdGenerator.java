/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.util.concurrent.atomic.AtomicLong;

public class RowIdGenerator {

    private final AtomicLong globalCounter;
    private final String generatorId;

    public RowIdGenerator(String generatorId) {
        this.generatorId = generatorId;
        this.globalCounter = new AtomicLong(0);
    }

    /**
     * Generates the next monotonic row ID.
     * Thread-safe and atomic operation.
     *
     * @return Next sequential row ID
     */
    public long getAndIncrementRowId() {
        return globalCounter.getAndIncrement();
    }
}
