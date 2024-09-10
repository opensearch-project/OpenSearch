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
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * Base class to define QueryGroup tasks
 */
@PublicApi(since = "2.18.0")
public class QueryGroupTask extends CancellableTask {

    private static final Logger logger = LogManager.getLogger(QueryGroupTask.class);
    public static final String QUERY_GROUP_ID_HEADER = "queryGroupId";
    public static final Supplier<String> DEFAULT_QUERY_GROUP_ID_SUPPLIER = () -> "DEFAULT_QUERY_GROUP";
    private LongSupplier nanoTimeSupplier;
    private String queryGroupId;

    public QueryGroupTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, description, parentTaskId, headers, NO_TIMEOUT, System::nanoTime);
    }

    public QueryGroupTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        TimeValue cancelAfterTimeInterval
    ) {
        super(id, type, action, description, parentTaskId, headers, cancelAfterTimeInterval);
    }

    public QueryGroupTask(
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
     * This method should always be called after calling setQueryGroupId at least once on this object
     * @return task queryGroupId
     */
    public final String getQueryGroupId() {
        if (queryGroupId == null) {
            logger.warn("QueryGroup _id can't be null, It should be set before accessing it. This is abnormal behaviour ");
        }
        return queryGroupId;
    }

    /**
     * sets the queryGroupId from threadContext into the task itself,
     * This method was defined since the queryGroupId can only be evaluated after task creation
     * @param threadContext current threadContext
     */
    public final void setQueryGroupId(final ThreadContext threadContext) {
        this.queryGroupId = Optional.ofNullable(threadContext)
            .map(threadContext1 -> threadContext1.getHeader(QUERY_GROUP_ID_HEADER))
            .orElse(DEFAULT_QUERY_GROUP_ID_SUPPLIER.get());
    }

    public long getElapsedTime() {
        return nanoTimeSupplier.getAsLong() - getStartTimeNanos();
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return false;
    }
}
