/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

/**
 * Executes the runnable on close
 */
public class ScopeImpl implements Scope {

    private Runnable runnableOnClose;

    /**
     * Creates Scope instance
     * @param runnableOnClose runnable to execute on scope close
     */
    public ScopeImpl(Runnable runnableOnClose) {
        this.runnableOnClose = runnableOnClose;
    }

    /**
     * Executes the runnable to end the scope
     */
    @Override
    public void close() {
        runnableOnClose.run();
    }
}
