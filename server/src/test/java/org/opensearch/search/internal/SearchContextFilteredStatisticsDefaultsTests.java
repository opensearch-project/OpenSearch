/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestSearchContext;

/** A {@link SearchContext} that does not support scoped statistics ignores a declared restriction. */
public class SearchContextFilteredStatisticsDefaultsTests extends OpenSearchTestCase {

    public void testDefaultsScopeNothing() {
        SearchContext context = new TestSearchContext(null);
        assertFalse(context.useFilteredStatistics());
        assertSame(context.aliasFilter(), context.filteredStatisticsFilter());

        context.visibleSubsetFilter(new TermQuery(new Term("dept", "visible")));
        assertFalse("declaring a restriction must not fail or take effect", context.useFilteredStatistics());
        assertSame(context.aliasFilter(), context.filteredStatisticsFilter());
    }
}
