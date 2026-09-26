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
    /** Separator between the {@code subfield|value} principal tokens carried by {@link #getThrottlePrincipal()}. */
    public static final String WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER = "\u001F";
    /**
     * Separator between the subfield name and its value inside a single principal token, as in {@code username|alice}.
     * Part of the contract with whichever plugin supplies the principal, so it is named rather than spelled inline.
     */
    public static final String WORKLOAD_GROUP_PRINCIPAL_SUBFIELD_DELIMITER = "|";
    public static final Supplier<String> DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER = () -> "DEFAULT_WORKLOAD_GROUP";
    private final LongSupplier nanoTimeSupplier;
    private String workloadGroupId;
    private boolean isWorkloadGroupSet = false;
    private volatile String throttlePrincipal;
    private volatile boolean throttleCounted;

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

    /**
     * Records the caller's principal for {@code username}/{@code role} throttling: {@code subfield|value} tokens (e.g.
     * {@code username|alice}) joined by {@link #WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER}, set by the WLM auto-tagging
     * filter. Held on the task rather than the {@link ThreadContext} so it is not serialized to shards/remote clusters and
     * cannot collide across concurrent sub-requests sharing a thread context (an {@code _msearch}).
     *
     * @param throttlePrincipal the joined principal tokens, or {@code null} when no extractor is installed
     */
    public void setThrottlePrincipal(final String throttlePrincipal) {
        this.throttlePrincipal = throttlePrincipal;
    }

    /**
     * The caller's principal for throttle bucket resolution, or {@code null} when unknown, in which case
     * {@code username}/{@code role} throttling fails open.
     */
    public String getThrottlePrincipal() {
        return throttlePrincipal;
    }

    /**
     * Marks this task's work as accounted for against a node-level throttle bucket, so a nested coordinator search on the
     * same node inherits the charge rather than taking a second permit (which would make the request compete with itself).
     * Set both when this task took a permit and when it was admitted free because its parent was already counted; never set
     * when the request was not throttled, so the charge lands on the first eligible search in a nested chain. Not cleared on
     * release (the task is short-lived), and release is driven by the returned
     * {@link org.opensearch.common.lease.Releasable}, not this flag, so marking cannot double-release.
     *
     * @param throttleCounted whether this task's work is accounted for against a throttle bucket
     */
    public void setThrottleCounted(final boolean throttleCounted) {
        this.throttleCounted = throttleCounted;
    }

    /**
     * Whether this task's work is accounted for against a node-level throttle bucket — either because it took the permit
     * itself or because its parent was already counted. {@code false} if it was never throttled.
     */
    public boolean isThrottleCounted() {
        return throttleCounted;
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
