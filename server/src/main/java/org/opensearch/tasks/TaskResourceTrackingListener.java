/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

/**
 * Listener for events related to resource tracking of a Task.
 */
public interface TaskResourceTrackingListener {

    /**
     * Invoked when a task execution is started on a thread.
     * @param task object
     * @param threadId on which execution of task started
     */
    default void onTaskExecutionStartedOnThread(Task task, long threadId) {}

    /**
     * Invoked when a task execution is finished on a thread.
     * @param task object
     * @param threadId on which execution of task finished.
     */
    default void onTaskExecutionFinishedOnThread(Task task, long threadId) {}

    /**
     * Invoked when a task's resource stats are updated.
     * @param task object
     */
    default void onTaskResourceStatsUpdated(Task task) {}

    /**
     * Invoked when a task's resource tracking is completed.
     * This happens when all of task's threads are marked inactive.
     * @param task object
     */
    default void onTaskResourceTrackingCompleted(Task task) {}
}
