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
 * Enum for task type
 */
@ExperimentalApi
public enum TaskType {
    /**
     * For all segment merge related tasks
     */
    MERGE,

    /**
     * For all snapshot related tasks
     */
    SNAPSHOT
}
