/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks;

/**
 * Class encapsulating Task id
 */
public class TaskId {

    /**
     * Id of the Task
     */
    String id;

    public TaskId(String id) {
        this.id = id;
    }
}
