/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed under
 * the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Process-wide registration point for the installed {@link ArrowBatchSourceExecutor}. */
public final class ArrowBatchSourceExecutorHolder {

    private static final AtomicReference<ArrowBatchSourceExecutor> EXECUTOR = new AtomicReference<>();

    private ArrowBatchSourceExecutorHolder() {}

    /** Installs or replaces the node's Arrow batch source executor. */
    public static void install(ArrowBatchSourceExecutor executor) {
        EXECUTOR.set(Objects.requireNonNull(executor, "executor"));
    }

    /** Removes {@code executor} if it is still the installed instance. */
    public static void remove(ArrowBatchSourceExecutor executor) {
        EXECUTOR.compareAndSet(executor, null);
    }

    public static boolean isAvailable() {
        return EXECUTOR.get() != null;
    }

    /** Returns the installed executor. */
    public static ArrowBatchSourceExecutor get() {
        ArrowBatchSourceExecutor executor = EXECUTOR.get();
        if (executor == null) {
            throw new IllegalStateException("No ArrowBatchSourceExecutor is installed");
        }
        return executor;
    }
}
