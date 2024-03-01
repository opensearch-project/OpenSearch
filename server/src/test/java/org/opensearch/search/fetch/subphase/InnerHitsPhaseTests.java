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

package org.opensearch.search.fetch.subphase;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InnerHitsPhaseTests extends OpenSearchTestCase {

    /*
    Returns mock search context reused across test methods
     */
    private SearchContext getMockSearchContext(final boolean hasInnerHits) {
        final QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.newFetchLookup()).thenReturn(mock(SearchLookup.class));

        final SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.hasInnerHits()).thenReturn(hasInnerHits);
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);

        return searchContext;
    }

    /*
    Validates that InnerHitsPhase processor is not initialized when no inner hits
     */
    public void testInnerHitsNull() {
        assertNull(new InnerHitsPhase(null).getProcessor(new FetchContext(getMockSearchContext(false))));
    }

    /*
    Validates that InnerHitsPhase processor is initialized when inner hits are present
     */
    public void testInnerHitsNonNull() {
        final SearchContext searchContext = getMockSearchContext(true);
        when(searchContext.innerHits()).thenReturn(new InnerHitsContext());

        assertNotNull(new InnerHitsPhase(null).getProcessor(new FetchContext(searchContext)));
    }

}
