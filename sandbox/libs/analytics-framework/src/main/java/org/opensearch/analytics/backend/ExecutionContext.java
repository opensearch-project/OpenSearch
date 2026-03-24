/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.opensearch.action.search.SearchShardTask;
import org.opensearch.analytics.plan.ResolvedPlan;
import org.opensearch.index.engine.DataFormatAwareEngine;

/**
 * Execution context carrying plan, reader, and delegation state through
 * the query execution lifecycle.
 *
 * @opensearch.internal
 */
public class ExecutionContext {

    private final ResolvedPlan plan;
    private final String tableName;
    private final DataFormatAwareEngine.DataFormatAwareReader reader;
    SearchShardTask task;

    public ExecutionContext(ResolvedPlan plan, String tableName, SearchShardTask task, DataFormatAwareEngine.DataFormatAwareReader reader) {
        this.plan = plan;
        this.tableName = tableName;
        this.task = task;
        this.reader = reader;
    }

    public SearchShardTask getTask() {
        return task;
    }

    public ResolvedPlan plan() {
        return plan;
    }

    public String getTableName() {
        return tableName;
    }

    public DataFormatAwareEngine.DataFormatAwareReader getReader() {
        return reader;
    }
}
