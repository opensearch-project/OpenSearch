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
import java.util.function.Supplier;

/**
 * This class is used to track query group level resource usage
 */
@PublicApi(since = "2.18.0")
public interface ResourceUsageCalculator {
    /**
     * calculates the current resource usage for the query group
     *
     * @param tasks        list of tasks in the query group
     * @param timeSupplier nano time supplier
     */
    double calculateResourceUsage(List<QueryGroupTask> tasks, Supplier<Long> timeSupplier);

    /**
     * calculates the task level resource usage
     * @param task         QueryGroupTask
     * @param timeSupplier in nano seconds unit
     * @return task level resource usage
     */
    double calculateTaskResourceUsage(QueryGroupTask task, Supplier<Long> timeSupplier);
}
