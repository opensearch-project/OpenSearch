/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.wlm.QueryGroupTask;

import java.util.List;

/**
 * This class is used to track query group level resource usage
 */
@PublicApi(since = "2.18.0")
public abstract class ResourceUsageCalculator {
    /**
     * calculates the current resource usage for the query group
     *
     * @param tasks        list of tasks in the query group
     */
    public abstract double calculateResourceUsage(List<QueryGroupTask> tasks);

    /**
     * calculates the task level resource usage
     * @param task         QueryGroupTask
     * @return task level resource usage
     */
    public abstract double calculateTaskResourceUsage(QueryGroupTask task);
}
