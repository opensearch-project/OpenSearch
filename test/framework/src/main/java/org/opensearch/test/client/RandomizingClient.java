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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.test.client;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchType;
import org.opensearch.client.Client;
import org.opensearch.client.FilterClient;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.common.unit.TimeValue;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/** A {@link Client} that randomizes request parameters. */
public class RandomizingClient extends FilterClient {

    private final SearchType defaultSearchType;
    private final String defaultPreference;
    private final int batchedReduceSize;
    private final int maxConcurrentShardRequests;
    private final int preFilterShardSize;
    private final boolean doTimeout;

    public RandomizingClient(Client client, Random random) {
        super(client);
        // we don't use the QUERY_AND_FETCH types that break quite a lot of tests
        // given that they return `size*num_shards` hits instead of `size`
        defaultSearchType = RandomPicks.randomFrom(random, Arrays.asList(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH));
        if (random.nextInt(10) == 0) {
            defaultPreference = RandomPicks.randomFrom(random, EnumSet.of(Preference.PRIMARY_FIRST, Preference.LOCAL)).type();
        } else if (random.nextInt(10) == 0) {
            String s = TestUtil.randomRealisticUnicodeString(random, 1, 10);
            defaultPreference = s.startsWith("_") ? null : s; // '_' is a reserved character
        } else {
            defaultPreference = null;
        }
        this.batchedReduceSize = 2 + random.nextInt(10);
        if (random.nextBoolean()) {
            this.maxConcurrentShardRequests = 1 + random.nextInt(1 << random.nextInt(8));
        } else {
            this.maxConcurrentShardRequests = -1; // randomly use the default
        }
        if (random.nextBoolean()) {
            preFilterShardSize = 1 + random.nextInt(1 << random.nextInt(7));
        } else {
            preFilterShardSize = -1;
        }
        doTimeout = random.nextBoolean();
    }

    @Override
    public SearchRequestBuilder prepareSearch(String... indices) {
        SearchRequestBuilder searchRequestBuilder = in.prepareSearch(indices)
            .setSearchType(defaultSearchType)
            .setPreference(defaultPreference)
            .setBatchedReduceSize(batchedReduceSize);
        if (maxConcurrentShardRequests != -1) {
            searchRequestBuilder.setMaxConcurrentShardRequests(maxConcurrentShardRequests);
        }
        if (preFilterShardSize != -1) {
            searchRequestBuilder.setPreFilterShardSize(preFilterShardSize);
        }
        if (doTimeout) {
            searchRequestBuilder.setTimeout(new TimeValue(1, TimeUnit.DAYS));
        }
        return searchRequestBuilder;
    }

    @Override
    public String toString() {
        return "randomized(" + super.toString() + ")";
    }

    public Client in() {
        return super.in();
    }

}
