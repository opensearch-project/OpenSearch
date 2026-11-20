/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.cache;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.cache.filteredstats.FilteredStatsCache;
import org.opensearch.index.cache.filteredstats.IndicesFilteredStatsCache;
import org.opensearch.index.cache.query.QueryCache;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** The filtered statistics cache is owned by {@link IndexCache}, which exposes, clears and closes it with the others. */
public class IndexCacheFilteredStatsTests extends OpenSearchTestCase {

    public void testExposesClearsAndClosesTheFilteredStatsCache() throws Exception {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("idx", Settings.EMPTY);
        FilteredStatsCache filteredStats = new FilteredStatsCache(
            indexSettings,
            new IndicesFilteredStatsCache(Settings.EMPTY, new NoopCircuitBreaker(CircuitBreaker.FIELDDATA))
        );
        QueryCache queryCache = mock(QueryCache.class);
        BitsetFilterCache bitsetCache = new BitsetFilterCache(indexSettings, mock(BitsetFilterCache.Listener.class));
        IndexCache cache = new IndexCache(indexSettings, queryCache, bitsetCache, filteredStats);

        assertSame(filteredStats, cache.filteredStatsCache());
        cache.clear("test");
        verify(queryCache).clear("test");
        cache.close();
        verify(queryCache).close();
    }

    public void testWorksWithoutAFilteredStatsCache() throws Exception {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("idx", Settings.EMPTY);
        IndexCache cache = new IndexCache(
            indexSettings,
            mock(QueryCache.class),
            new BitsetFilterCache(indexSettings, mock(BitsetFilterCache.Listener.class))
        );
        assertNull(cache.filteredStatsCache());
        cache.clear("test");
        cache.close();
    }
}
