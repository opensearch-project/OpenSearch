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
    public static final Supplier<String> DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER = () -> "DEFAULT_WORKLOAD_GROUP";
    private final LongSupplier nanoTimeSupplier;
    private String workloadGroupId;
    private boolean isWorkloadGroupSet = false;
    private volatile String throttlePrincipal;
    private volatile String heldThrottleBucket;

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
     * Records the caller's principal for {@code username}/{@code role} throttling: {@code subfield|value} tokens
     * (e.g. {@code username|alice}) joined by {@link #WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER}. Set on the coordinator
     * by the WLM auto-tagging action filter, from the security plugin's principal extractor, before the action executes.
     * <p>
     * Deliberately held on the task rather than in the {@link ThreadContext}: a ThreadContext request header is
     * serialized onto every outgoing transport request, which would ship the caller's identity to every shard and to
     * remote clusters in a cross-cluster search even though only the coordinator reads it. A task field is also not
     * something a client can supply, and is naturally per-request, so concurrent sub-requests sharing one thread context
     * (an {@code _msearch}) cannot collide.
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
     * Records the throttle bucket this task successfully took a permit for, so a nested coordinator search issued
     * while this one is in flight can recognise that its bucket is already paid for and skip admission. Set by
     * {@code WorkloadGroupService#acquireThrottleOrReject} on a successful acquire only.
     * <p>
     * Deliberately not cleared on release: the value is scoped to the task, which is unregistered when the request
     * finishes, and a rewrite round that issues a nested search after the outer permit has been released must still
     * be recognised as nested rather than charged a fresh permit.
     *
     * @param heldThrottleBucket the bucket key a permit is held for
     */
    public void setHeldThrottleBucket(final String heldThrottleBucket) {
        this.heldThrottleBucket = heldThrottleBucket;
    }

    /**
     * The throttle bucket this task holds (or held) a permit for, or {@code null} if it was never throttled.
     */
    public String getHeldThrottleBucket() {
        return heldThrottleBucket;
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
