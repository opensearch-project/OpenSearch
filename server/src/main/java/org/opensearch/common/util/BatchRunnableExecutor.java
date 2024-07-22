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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.TimeoutAwareRunnable;
import org.opensearch.core.common.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The executor that executes a batch of {@link TimeoutAwareRunnable} and triggers a timeout based on {@link TimeValue} timeout
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
        Collections.shuffle(timeoutAwareRunnables);
        long startTime = System.nanoTime();
        if (CollectionUtils.isEmpty(timeoutAwareRunnables)) {
            return;
        }
        for (TimeoutAwareRunnable workQueue : timeoutAwareRunnables) {
            if (timeoutSupplier.get().nanos() < 0 || System.nanoTime() - startTime < timeoutSupplier.get().nanos()) {
                workQueue.run();
            } else {
                logger.debug("Executing timeout for runnable of size [{}]", timeoutAwareRunnables.size());
                workQueue.onTimeout();
            }
        }
        logger.debug(
            "Time taken to execute timed runnables in this cycle:[{}ms]",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)
        );
    }

}
