/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.offline_tasks.client;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.offline_tasks.task.TaskStatus;

/**
 * Holder interface for various params different implementations might add for list calls
 * A typical example would be pagination related params
 *
 * Since Each implementation of this abstraction can host multiple different params,
 * it is recommended to use Builder Pattern for construction of the objects
 */
@ExperimentalApi
public abstract class TaskListQueryParams {

    /**
     * Depicts the start page number for the list call.
     *
     * @see TaskClient#getTasks(TaskStatus, TaskListQueryParams)
     */
    private final int startPageNumber;

    /**
     * Depicts the page size for the list call.
     *
     * @see TaskClient#getTasks(TaskStatus, TaskListQueryParams)
     */
    private final int pageSize;

    /**
     * Constructor for ListTaskParams
     * @param startPageNumber start page number for the list call
     * @param pageSize page size for the list call
     */
    private TaskListQueryParams(int startPageNumber, int pageSize) {
        this.startPageNumber = startPageNumber;
        this.pageSize = pageSize;
    }

    /**
     * Get pageSize
     * @return pageSize for the list call
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Get startPageNumber
     * @return startPageNumber for the list call
     */
    public int getStartPageNumber() {
        return startPageNumber;
    }

    /**
     * Builder class for ListTaskParams. Since this class can grow and host multiple params
     */
    public abstract static class Builder {
        /**
         * Depicts the start page number for the list call.
         *
         * @see TaskClient#getTasks(TaskStatus, TaskListQueryParams)
         */
        private int startPageNumber = 1;

        /**
         * Depicts the page size for the list call.
         *
         * @see TaskClient#getTasks(TaskStatus, TaskListQueryParams)
         */
        private int pageSize = 50;

        public abstract TaskListQueryParams build();

        public Builder setStartPageNumber(int startPageNumber) {
            this.startPageNumber = startPageNumber;
            return this;
        }

        public Builder setPageSize(int pageSize){
            this.pageSize = pageSize;
            return this;
        }
    }
}
