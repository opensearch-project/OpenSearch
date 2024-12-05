/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.transport.Transport;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * SearchPhaseContext for tests
 */
public final class MockSearchPhaseContext implements SearchPhaseContext {
    private static final Logger logger = LogManager.getLogger(MockSearchPhaseContext.class);
    final AtomicReference<Throwable> phaseFailure = new AtomicReference<>();
    final int numShards;
    final AtomicInteger numSuccess;
    final List<ShardSearchFailure> failures = Collections.synchronizedList(new ArrayList<>());
    SearchTransportService searchTransport;
    final Set<ShardSearchContextId> releasedSearchContexts = new HashSet<>();
    final SearchRequest searchRequest;
    final AtomicReference<SearchResponse> searchResponse = new AtomicReference<>();
    final SearchPhase currentPhase;

    public MockSearchPhaseContext(int numShards) {
        this(numShards, new SearchRequest());
    }

    public MockSearchPhaseContext(int numShards, SearchRequest searchRequest) {
        this(numShards, searchRequest, null);
    }

    public MockSearchPhaseContext(int numShards, SearchRequest searchRequest, SearchPhase currentPhase) {
        this.numShards = numShards;
        this.searchRequest = searchRequest;
        this.currentPhase = currentPhase;
        numSuccess = new AtomicInteger(numShards);
    }

    public MockSearchPhaseContext(int numShards, SearchPhase currentPhase) {
        this(numShards, new SearchRequest(), currentPhase);
    }

    public void assertNoFailure() {
        if (phaseFailure.get() != null) {
            throw new AssertionError(phaseFailure.get());
        }
    }

    @Override
    public int getNumShards() {
        return numShards;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public SearchTask getTask() {
        return new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
    }

    @Override
    public SearchRequest getRequest() {
        return searchRequest;
    }

    @Override
    public SearchPhase getCurrentPhase() {
        return currentPhase;
    }

    @Override
    public void sendSearchResponse(InternalSearchResponse internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults) {
        String scrollId = getRequest().scroll() != null ? TransportSearchHelper.buildScrollId(queryResults, Version.CURRENT) : null;
        String searchContextId = getRequest().pointInTimeBuilder() != null
            ? TransportSearchHelper.buildScrollId(queryResults, Version.CURRENT)
            : null;
        searchResponse.set(
            new SearchResponse(
                internalSearchResponse,
                scrollId,
                numShards,
                numSuccess.get(),
                0,
                0,
                failures.toArray(ShardSearchFailure.EMPTY_ARRAY),
                SearchResponse.Clusters.EMPTY,
                searchContextId
            )
        );
    }

    @Override
    public void onPhaseFailure(SearchPhase phase, String msg, Throwable cause) {
        phaseFailure.set(cause);
    }

    @Override
    public void onShardFailure(int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e) {
        failures.add(new ShardSearchFailure(e, shardTarget));
        numSuccess.decrementAndGet();
    }

    @Override
    public Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return null; // null is ok here for this test
    }

    @Override
    public SearchTransportService getSearchTransport() {
        Assert.assertNotNull(searchTransport);
        return searchTransport;
    }

    @Override
    public ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt) {
        Assert.fail("should not be called");
        return null;
    }

    @Override
    public void executeNextPhase(SearchPhase currentPhase, SearchPhase nextPhase) {
        try {
            nextPhase.run();
        } catch (Exception e) {
            onPhaseFailure(nextPhase, "phase failed", e);
        }
    }

    @Override
    public void addReleasable(Releasable releasable) {
        // Noop
    }

    /**
     * Set the resource usage info for this phase
     */
    @Override
    public void setPhaseResourceUsages() {
        // Noop
    }

    @Override
    public void execute(Runnable command) {
        command.run();
    }

    @Override
    public void onFailure(Exception e) {
        Assert.fail("should not be called");
    }

    @Override
    public void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection, OriginalIndices originalIndices) {
        releasedSearchContexts.add(contextId);
    }
}
