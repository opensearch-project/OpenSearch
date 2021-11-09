/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.tasks.TaskStatsContext;

/**
 * A simple listener that logs all stats published in the tasks
 */
public class TaskStatsLogger implements TaskStatConsumer {
    private static final Logger LOGGER = LogManager.getLogger(TaskStatsLogger.class);

    @Override
    public void taskStatConsumed(TaskStatsContext taskStatsContext) {
        LOGGER.debug("Task stats: [{}] for task type=[{}]", taskStatsContext.getAllStats(), taskStatsContext.getType());
    }
}
