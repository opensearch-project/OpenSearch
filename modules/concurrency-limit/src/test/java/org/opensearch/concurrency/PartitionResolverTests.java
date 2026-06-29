/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class PartitionResolverTests extends OpenSearchTestCase {

    private static SearchRequestContext ctxFor(ActionRequest request) {
        return new SearchRequestContext(null, "indices:data/read/search", request);
    }

    private static SearchRequest aggSearch() {
        return new SearchRequest().source(new SearchSourceBuilder().aggregation(AggregationBuilders.max("m").field("f")));
    }

    private static SearchRequest plainSearch() {
        return new SearchRequest().source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()));
    }

    // -------------------------------------------------------------------------
    // byHeader
    // -------------------------------------------------------------------------

    public void testByHeaderReadsTierHeader() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(ActionConcurrencyLimitPlugin.TIER_HEADER, "premium");
        PartitionResolver r = PartitionResolver.build("byHeader", Settings.EMPTY, threadContext);
        assertEquals("premium", r.resolve(ctxFor(plainSearch())));
    }

    public void testByHeaderReturnsNullWhenHeaderMissing() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        PartitionResolver r = PartitionResolver.build("byHeader", Settings.EMPTY, threadContext);
        assertNull(r.resolve(ctxFor(plainSearch())));
    }

    // -------------------------------------------------------------------------
    // bySearchType
    // -------------------------------------------------------------------------

    public void testAggregationSearchRoutesToAggregationPartition() {
        PartitionResolver r = new PartitionResolver.BySearchTypePartitionResolver(Settings.EMPTY);
        assertEquals("aggregation", r.resolve(ctxFor(aggSearch())));
    }

    public void testPlainQuerySearchRoutesToFilterPartition() {
        PartitionResolver r = new PartitionResolver.BySearchTypePartitionResolver(Settings.EMPTY);
        assertEquals("filter", r.resolve(ctxFor(plainSearch())));
    }

    public void testNullSourceRoutesToFilter() {
        PartitionResolver r = new PartitionResolver.BySearchTypePartitionResolver(Settings.EMPTY);
        // bare SearchRequest has a null source
        assertEquals("filter", r.resolve(ctxFor(new SearchRequest())));
    }

    public void testQueryWithAggregationsCountsAsAggregation() {
        SearchRequest req = new SearchRequest().source(
            new SearchSourceBuilder().query(new MatchAllQueryBuilder()).aggregation(AggregationBuilders.terms("t").field("f"))
        );
        PartitionResolver r = new PartitionResolver.BySearchTypePartitionResolver(Settings.EMPTY);
        assertEquals("aggregation", r.resolve(ctxFor(req)));
    }

    public void testCustomPartitionNames() {
        Settings config = Settings.builder().put("aggregation", "heavy").put("filter", "light").build();
        PartitionResolver r = new PartitionResolver.BySearchTypePartitionResolver(config);
        assertEquals("heavy", r.resolve(ctxFor(aggSearch())));
        assertEquals("light", r.resolve(ctxFor(plainSearch())));
    }

    public void testNonSearchRequestResolvesToNull() {
        ActionRequest notASearch = new ActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        };
        PartitionResolver r = new PartitionResolver.BySearchTypePartitionResolver(Settings.EMPTY);
        assertNull(r.resolve(ctxFor(notASearch)));
    }

    public void testFactoryBuildsBySearchType() {
        PartitionResolver r = PartitionResolver.build("bySearchType", Settings.EMPTY, null);
        assertTrue(r instanceof PartitionResolver.BySearchTypePartitionResolver);
        assertEquals("aggregation", r.resolve(ctxFor(aggSearch())));
        assertEquals("filter", r.resolve(ctxFor(plainSearch())));
    }

    public void testFactoryUnknownTypeResolvesToNull() {
        PartitionResolver r = PartitionResolver.build("byTenant", Settings.EMPTY, null);
        assertNull(r.resolve(ctxFor(aggSearch())));
    }
}
