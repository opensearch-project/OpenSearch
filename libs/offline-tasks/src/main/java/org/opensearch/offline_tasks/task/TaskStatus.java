/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks.task;

/**
 * Task status enum
 */
public enum TaskStatus {

    /**
     * TaskStatus of an in progress Task
     */
    ACTIVE,

    /**
     * TaskStatus of a finished Task
     */
    COMPLETED,

    /**
     * TaskStatus of a Task which failed in 1 or more attempts
     */
    FAILED

}
