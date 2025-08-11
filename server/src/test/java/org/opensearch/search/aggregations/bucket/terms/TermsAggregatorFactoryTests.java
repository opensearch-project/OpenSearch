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

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TermsAggregatorFactoryTests extends OpenSearchTestCase {
    public void testPickEmpty() throws Exception {
        AggregatorFactories empty = mock(AggregatorFactories.class);
        SearchContext context = mock(SearchContext.class);
        when(empty.countAggregators()).thenReturn(0);
        assertThat(
            TermsAggregatorFactory.pickSubAggCollectMode(empty, randomInt(), randomInt(), context),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST)
        );
    }

    public void testPickNonEempty() {
        AggregatorFactories nonEmpty = mock(AggregatorFactories.class);
        SearchContext context = mock(SearchContext.class);
        when(nonEmpty.countAggregators()).thenReturn(1);
        assertThat(
            TermsAggregatorFactory.pickSubAggCollectMode(nonEmpty, Integer.MAX_VALUE, -1, context),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST)
        );
        assertThat(
            TermsAggregatorFactory.pickSubAggCollectMode(nonEmpty, 10, -1, context),
            equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST)
        );
        assertThat(
            TermsAggregatorFactory.pickSubAggCollectMode(nonEmpty, 10, 5, context),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST)
        );
        assertThat(
            TermsAggregatorFactory.pickSubAggCollectMode(nonEmpty, 10, 10, context),
            equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST)
        );
        assertThat(
            TermsAggregatorFactory.pickSubAggCollectMode(nonEmpty, 10, 100, context),
            equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST)
        );
        assertThat(
            TermsAggregatorFactory.pickSubAggCollectMode(nonEmpty, 1, 2, context),
            equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST)
        );
        assertThat(
            TermsAggregatorFactory.pickSubAggCollectMode(nonEmpty, 1, 100, context),
            equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST)
        );
    }
}
