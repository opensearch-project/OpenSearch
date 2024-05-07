/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks.task;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Class encapsulating Task identifier
 */
@ExperimentalApi
public class TaskId {

    /**
     * Identified of the Task
     */
    String id;

    /**
     * Constructor to initialize TaskId
     * @param id
     */
    public TaskId(String id) {
        this.id = id;
    }
}
