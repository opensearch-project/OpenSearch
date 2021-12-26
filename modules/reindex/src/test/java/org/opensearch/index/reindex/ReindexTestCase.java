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

package org.opensearch.index.reindex;

import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.opensearch.test.OpenSearchIntegTestCase.Scope.SUITE;

/**
 * Base test case for integration tests against the reindex plugin.
 */
@ClusterScope(scope = SUITE)
public abstract class ReindexTestCase extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class);
    }

    protected ReindexRequestBuilder reindex() {
        return new ReindexRequestBuilder(client(), ReindexAction.INSTANCE);
    }

    protected UpdateByQueryRequestBuilder updateByQuery() {
        return new UpdateByQueryRequestBuilder(client(), UpdateByQueryAction.INSTANCE);
    }

    protected DeleteByQueryRequestBuilder deleteByQuery() {
        return new DeleteByQueryRequestBuilder(client(), DeleteByQueryAction.INSTANCE);
    }

    protected RethrottleRequestBuilder rethrottle() {
        return new RethrottleRequestBuilder(client(), RethrottleAction.INSTANCE);
    }

    public static BulkIndexByScrollResponseMatcher matcher() {
        return new BulkIndexByScrollResponseMatcher();
    }

    static int randomSlices(int min, int max) {
        if (randomBoolean()) {
            return AbstractBulkByScrollRequest.AUTO_SLICES;
        } else {
            return between(min, max);
        }
    }

    static int randomSlices() {
        return randomSlices(2, 10);
    }

    /**
     * Figures out how many slices the request handling will use
     */
    protected int expectedSlices(int requestSlices, Collection<String> indices) {
        if (requestSlices == AbstractBulkByScrollRequest.AUTO_SLICES) {
            int leastNumShards = Collections.min(
                indices.stream().map(sourceIndex -> getNumShards(sourceIndex).numPrimaries).collect(Collectors.toList())
            );
            return Math.min(leastNumShards, BulkByScrollParallelizationHelper.AUTO_SLICE_CEILING);
        } else {
            return requestSlices;
        }
    }

    protected int expectedSlices(int requestSlices, String index) {
        return expectedSlices(requestSlices, singleton(index));
    }

    /**
     * Figures out how many slice statuses to expect in the response
     */
    protected int expectedSliceStatuses(int requestSlices, Collection<String> indices) {
        int slicesConfigured = expectedSlices(requestSlices, indices);

        if (slicesConfigured > 1) {
            return slicesConfigured;
        } else {
            return 0;
        }
    }

    protected int expectedSliceStatuses(int slicesConfigured, String index) {
        return expectedSliceStatuses(slicesConfigured, singleton(index));
    }
}
