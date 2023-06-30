/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

/**
 * An auto-closeable that represents scope of the span.
 * It is recommended that you use this class with a try-with-resources block:
 */
public interface Scope extends AutoCloseable {
    /**
     * No-op Scope implementation
     */
    Scope NO_OP = () -> {};

    /**
     * closes the scope
     */
    @Override
    void close();
}
