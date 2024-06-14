/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query_group.cancellation;

/**
 * This interface is used to identify and cancel the violating tasks in a QueryGroup
 */
public interface QueryGroupRequestCanceller {
    /**
     * Cancels the tasks from conteded QueryGroups
     */
    void cancelViolatingTasks();
}
