/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import java.util.List;

public class DatafusionQuery {
    private String indexName;
    private final byte[] substraitBytes;

    // List of Search executors which returns a result iterator which contains row id which can be joined in datafusion
    private final List<SearchExecutor> searchExecutors;
    private Boolean isFetchPhase;
    private List<Long> queryPhaseRowIds;
    private List<String> includeFields;
    private List<String> excludeFields;
    private boolean isAggregationQuery;

    public DatafusionQuery(String indexName, byte[] substraitBytes, List<SearchExecutor> searchExecutors, boolean isAggregationQuery) {
        this.indexName = indexName;
        this.substraitBytes = substraitBytes;
        this.searchExecutors = searchExecutors;
        this.isFetchPhase = false;
        this.isAggregationQuery = isAggregationQuery;
    }

    public void setSource(List<String> includeFields, List<String> excludeFields) {
        this.includeFields = includeFields;
        this.excludeFields = excludeFields;
    }

    public void setFetchPhaseContext(List<Long> queryPhaseRowIds) {
        this.queryPhaseRowIds = queryPhaseRowIds;
        this.isFetchPhase = true;
    }

    public boolean isFetchPhase() {
        return this.isFetchPhase;
    }

    public boolean isAggregationQuery() {
        return isAggregationQuery;
    }

    public void setAggregationQuery(boolean aggregationQuery) {
        isAggregationQuery = aggregationQuery;
    }

    public List<Long> getQueryPhaseRowIds() {
        return this.queryPhaseRowIds;
    }

    public List<String> getIncludeFields() {
        return this.includeFields;
    }

    public List<String> getExcludeFields() {
        return this.excludeFields;
    }

    public byte[] getSubstraitBytes() {
        return substraitBytes;
    }

    public List<SearchExecutor> getSearchExecutors() {
        return searchExecutors;
    }

    public String getIndexName() {
        return indexName;
    }
}
