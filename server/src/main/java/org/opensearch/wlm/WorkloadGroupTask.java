/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * Base class to define WorkloadGroup tasks
 */
@PublicApi(since = "2.18.0")
public class WorkloadGroupTask extends CancellableTask {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupTask.class);
    public static final String WORKLOAD_GROUP_ID_HEADER = "workloadGroupId";
    public static final Supplier<String> DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER = () -> "DEFAULT_WORKLOAD_GROUP";
    private final LongSupplier nanoTimeSupplier;
    private String workloadGroupId;
    private boolean isWorkloadGroupSet = false;

    public WorkloadGroupTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, description, parentTaskId, headers, NO_TIMEOUT, System::nanoTime);
    }

    public WorkloadGroupTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        TimeValue cancelAfterTimeInterval
    ) {
        this(id, type, action, description, parentTaskId, headers, cancelAfterTimeInterval, System::nanoTime);
    }

    public WorkloadGroupTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        TimeValue cancelAfterTimeInterval,
        LongSupplier nanoTimeSupplier
    ) {
        super(id, type, action, description, parentTaskId, headers, cancelAfterTimeInterval);
        this.nanoTimeSupplier = nanoTimeSupplier;
    }

    /**
     * This method should always be called after calling setWorkloadGroupId at least once on this object
     * @return task workloadGroupId
     */
    public final String getWorkloadGroupId() {
        if (workloadGroupId == null) {
            logger.warn("WorkloadGroup _id can't be null, It should be set before accessing it. This is abnormal behaviour ");
        }
        return workloadGroupId;
    }

    /**
     * sets the workloadGroupId from threadContext into the task itself,
     * This method was defined since the workloadGroupId can only be evaluated after task creation
     * @param threadContext current threadContext
     */
    public final void setWorkloadGroupId(final ThreadContext threadContext) {
        isWorkloadGroupSet = true;
        if (threadContext != null && threadContext.getHeader(WORKLOAD_GROUP_ID_HEADER) != null) {
            this.workloadGroupId = threadContext.getHeader(WORKLOAD_GROUP_ID_HEADER);
        } else {
            this.workloadGroupId = DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get();
        }
    }

    public long getElapsedTime() {
        return nanoTimeSupplier.getAsLong() - getStartTimeNanos();
    }

    public boolean isWorkloadGroupSet() {
        return isWorkloadGroupSet;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return false;
    }
}
