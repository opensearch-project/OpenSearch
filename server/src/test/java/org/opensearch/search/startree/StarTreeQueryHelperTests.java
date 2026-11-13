/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StarTreeQueryHelperTests extends OpenSearchTestCase {

    /** A context that star-tree supports, apart from the request it is given. */
    private static SearchContext contextWithRequest(ShardSearchRequest request) {
        SearchContext context = mock(SearchContext.class);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.isCompositeIndexPresent()).thenReturn(true);
        when(context.mapperService()).thenReturn(mapperService);
        when(context.aggregations()).thenReturn(mock(SearchContextAggregations.class));
        when(context.request()).thenReturn(request);
        return context;
    }

    private static ShardSearchRequest requestWith(AliasFilter aliasFilter) {
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        when(request.getAliasFilter()).thenReturn(aliasFilter);
        return request;
    }

    public void testContextWithoutARequestIsSupported() {
        assertTrue(StarTreeQueryHelper.isStarTreeSupported(contextWithRequest(null)));
    }

    public void testAliasWithoutAFilterIsSupported() {
        assertTrue(StarTreeQueryHelper.isStarTreeSupported(contextWithRequest(requestWith(null))));
        assertTrue(StarTreeQueryHelper.isStarTreeSupported(contextWithRequest(requestWith(new AliasFilter(null, "plain")))));
    }

    public void testFilteredAliasIsNotSupported() {
        AliasFilter filtered = new AliasFilter(QueryBuilders.termQuery("f", "v"), "filtered");
        assertFalse(StarTreeQueryHelper.isStarTreeSupported(contextWithRequest(requestWith(filtered))));
    }
}
