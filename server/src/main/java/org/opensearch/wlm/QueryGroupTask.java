/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;


import java.util.Map;

import static org.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * Base class to define QueryGroup tasks
 */
public class QueryGroupTask extends CancellableTask {

    private String queryGroupId;

    public QueryGroupTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        this(id, type, action, description, parentTaskId, headers, NO_TIMEOUT);
    }

    public QueryGroupTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers, TimeValue cancelAfterTimeInterval) {
        super(id, type, action, description, parentTaskId, headers, cancelAfterTimeInterval);
    }

    public String getQueryGroupId() {
        return queryGroupId;
    }

    public void setQueryGroupId(final ThreadContext threadContext) {
        this.queryGroupId = QueryGroupConstants.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get();

        if (threadContext != null
            && threadContext.getHeader(QueryGroupConstants.QUERY_GROUP_ID_HEADER) != null) {
            this.queryGroupId = threadContext.getHeader(QueryGroupConstants.QUERY_GROUP_ID_HEADER);
        }
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return false;
    }
}
