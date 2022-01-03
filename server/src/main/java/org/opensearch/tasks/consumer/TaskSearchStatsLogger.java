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
import org.opensearch.tasks.Task;

import java.util.function.Consumer;

/**
 * A simple listener that logs resource information of search tasks
 */
public class TaskSearchStatsLogger implements Consumer<Task> {
    private static final Logger LOGGER = LogManager.getLogger("task.detailslog");

    /**
     * Called when task is unregistered and task has stats present.
     */
    @Override
    public void accept(Task task) {
        if (task.getAction().contains("search")) {
            LOGGER.info(new TaskDetailsMessage(task));
        }
    }
}
