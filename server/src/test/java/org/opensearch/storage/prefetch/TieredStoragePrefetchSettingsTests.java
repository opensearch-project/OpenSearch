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
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.HashSet;
import java.util.Set;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;

public class TieredStoragePrefetchSettingsTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("TieredStoragePrefetchSettingsTests");
        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(TieredStoragePrefetchSettings.READ_AHEAD_BLOCK_COUNT);
        clusterSettingsToAdd.add(TieredStoragePrefetchSettings.STORED_FIELDS_PREFETCH_ENABLED_SETTING);
        clusterService = ClusterServiceUtils.createClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd),
            threadPool
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testDefaultSettings() {
        TieredStoragePrefetchSettings settings = new TieredStoragePrefetchSettings(clusterService.getClusterSettings());
        assertEquals(TieredStoragePrefetchSettings.DEFAULT_READ_AHEAD_BLOCK_COUNT, settings.getReadAheadBlockCount());
        assertEquals(TieredStoragePrefetchSettings.READ_AHEAD_ENABLE_FILE_FORMATS, settings.getReadAheadEnableFileFormats());
        assertEquals(true, settings.isStoredFieldsPrefetchEnabled());
    }

    public void testUpdateAfterGetDefaultSettings() {
        TieredStoragePrefetchSettings tieringServicePrefetchSettings = new TieredStoragePrefetchSettings(
            clusterService.getClusterSettings()
        );
        assertEquals(tieringServicePrefetchSettings.getReadAheadBlockCount(), TieredStoragePrefetchSettings.DEFAULT_READ_AHEAD_BLOCK_COUNT);
        assertEquals(tieringServicePrefetchSettings.isStoredFieldsPrefetchEnabled(), true);
        Settings settings = Settings.builder().put(TieredStoragePrefetchSettings.READ_AHEAD_BLOCK_COUNT.getKey(), 10).build();
        clusterService.getClusterSettings().applySettings(settings);
        assertEquals(tieringServicePrefetchSettings.getReadAheadBlockCount(), 10);
    }
}
