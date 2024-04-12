/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resource_limit_group;

/**
 * This interface is used to identify and completely remove deleted resourceLimitGroups which has been marked as deleted
 * previously but had the tasks running at the time of deletion request
 */
public interface ResourceLimitGroupPruner {
    /**
     * remove the deleted resourceLimitGroups from the system once all the tasks in those resourceLimitGroups are completed/cancelled
     */
    void pruneResourceLimitGroup();
}
