/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks.consumer;

import org.opensearch.tasks.TaskStatsContext;

/**
 * This listener is notified whenever an task is completed and has stats present
 */
public interface TaskStatConsumer {

    /**
     * Called when task is unregistered and task has stats present.
     */
    void taskStatConsumed(TaskStatsContext taskStatsContext);
}
