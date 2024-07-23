/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Randomness;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.TimeoutAwareRunnable;
import org.opensearch.core.common.util.CollectionUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A {@link Runnable} that iteratively executes a batch of {@link TimeoutAwareRunnable}s. If the elapsed time exceeds the timeout defined by {@link TimeValue} timeout, then all subsequent {@link TimeoutAwareRunnable}s will have their {@link TimeoutAwareRunnable#onTimeout} method invoked and will not be run.
 *
 * @opensearch.internal
 */
public class BatchRunnableExecutor implements Runnable {

    private final Supplier<TimeValue> timeoutSupplier;

    private final List<TimeoutAwareRunnable> timeoutAwareRunnables;

    private static final Logger logger = LogManager.getLogger(BatchRunnableExecutor.class);

    public BatchRunnableExecutor(List<TimeoutAwareRunnable> timeoutAwareRunnables, Supplier<TimeValue> timeoutSupplier) {
        this.timeoutSupplier = timeoutSupplier;
        this.timeoutAwareRunnables = timeoutAwareRunnables;
    }

    @Override
    public void run() {
        logger.debug("Starting execution of runnable of size [{}]", timeoutAwareRunnables.size());
        long startTime = System.nanoTime();
        if (timeoutAwareRunnables.isEmpty()) {
            return;
        }
        Randomness.shuffle(timeoutAwareRunnables);
        for (TimeoutAwareRunnable runnable : timeoutAwareRunnables) {
            if (timeoutSupplier.get().nanos() < 0 || System.nanoTime() - startTime < timeoutSupplier.get().nanos()) {
                runnable.run();
            } else {
                logger.debug("Executing timeout for runnable of size [{}]", timeoutAwareRunnables.size());
                runnable.onTimeout();
            }
        }
        logger.debug(
            "Time taken to execute timed runnables in this cycle:[{}ms]",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)
        );
    }

}
