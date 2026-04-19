/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds references to IndexInput clones/slices created during a thread pool task execution.
 * Bound via {@link ScopedValue} in {@link OpenSearchThreadPoolExecutor} so that all
 * registered IndexInputs are closed when the task completes.
 *
 * <p>For slices, {@code close()} only unpins the current block without setting
 * {@code isOpen=false}, so merge threads that hold references can still use them.
 *
 * @opensearch.internal
 */
public final class IndexInputScope {

    private static final Logger logger = LogManager.getLogger(IndexInputScope.class);

    public static final ScopedValue<IndexInputScope> SCOPE = ScopedValue.newInstance();

    private final List<Closeable> inputs = new ArrayList<>();

    /**
     * Register an IndexInput (clone or slice) for cleanup when this scope ends.
     */
    public void register(Closeable input) {
        inputs.add(input);
    }

    /**
     * Close all registered IndexInputs. Called in the finally block of task execution.
     */
    public void closeAll() {
        for (int i = inputs.size() - 1; i >= 0; i--) {
            try {
                inputs.get(i).close();
            } catch (IOException e) {
                logger.trace("failed to close IndexInput in scope", e);
            }
        }
        inputs.clear();
    }
}
