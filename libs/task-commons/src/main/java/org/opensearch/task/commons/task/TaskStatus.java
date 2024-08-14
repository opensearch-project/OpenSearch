/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.task.commons.task;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Task status enum
 */
@ExperimentalApi
public enum TaskStatus {

    /**
     * TaskStatus of a Task which is not yet assigned to or picked up by a worker
     */
    UNASSIGNED,

    /**
     * TaskStatus of a Task which is assigned to or picked up by a worker but hasn't started execution yet.
     * This status confirms that a worker will execute this task and no other worker should pick it up.
     */
    ASSIGNED,

    /**
     * TaskStatus of an in progress Task
     */
    ACTIVE,

    /**
     * TaskStatus of a finished Task
     */
    SUCCESS,

    /**
     * TaskStatus of a Task which failed in 1 or more attempts
     */
    FAILED,

    /**
     * TaskStatus of a cancelled Task
     */
    CANCELLED
}
