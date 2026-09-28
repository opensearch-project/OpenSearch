/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.prefetch;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StoredFieldsPrefetchTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private SearchContext searchContext;
    private TieredStoragePrefetchSettings tieredStoragePrefetchSettings;
    private StoredFieldsPrefetch storedFieldsPrefetch;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(TieredStoragePrefetchSettings.READ_AHEAD_BLOCK_COUNT);
        clusterSettingsToAdd.add(TieredStoragePrefetchSettings.STORED_FIELDS_PREFETCH_ENABLED_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);
        threadPool = new TestThreadPool("StoredFieldsPrefetchTests");
        clusterService = ClusterServiceUtils.createClusterService(Settings.EMPTY, clusterSettings, threadPool);
        this.tieredStoragePrefetchSettings = new TieredStoragePrefetchSettings(clusterService.getClusterSettings());
        searchContext = mock(SearchContext.class);
        storedFieldsPrefetch = new StoredFieldsPrefetch(getPrefetchSettingsSupplier());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public Supplier<TieredStoragePrefetchSettings> getPrefetchSettingsSupplier() {
        return () -> this.tieredStoragePrefetchSettings;
    }

    public void testOnPreFetchPhase_WhenPrefetchDisabled() {
        Settings settings = Settings.builder()
            .put(TieredStoragePrefetchSettings.STORED_FIELDS_PREFETCH_ENABLED_SETTING.getKey(), false)
            .build();
        clusterService.getClusterSettings().applySettings(settings);
        storedFieldsPrefetch.onPreFetchPhase(searchContext);
        verify(searchContext, never()).docIdsToLoadSize();
    }

    public void testOnPreFetchPhase_WhenPrefetchEnabled_ZeroDocs() {
        // Prefetch is enabled by default, but no docs to load
        when(searchContext.docIdsToLoadSize()).thenReturn(0);
        storedFieldsPrefetch.onPreFetchPhase(searchContext);
        // docIdsToLoadSize is called in the log statement and in the loop condition
        verify(searchContext, times(2)).docIdsToLoadSize();
    }
}
