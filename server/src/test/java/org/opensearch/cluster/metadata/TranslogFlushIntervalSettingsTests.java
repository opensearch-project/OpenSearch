/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

/**
 * Tests for translog flush interval settings update with and without composite index
 */
public class TranslogFlushIntervalSettingsTests extends OpenSearchTestCase {

    Settings settings = Settings.builder()
        .put(CompositeIndexSettings.COMPOSITE_INDEX_MAX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "130mb")
        .build();
    ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    public void testValidSettings() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "50mb")
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .build();

        // This should not throw an exception
        MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings);
    }

    public void testDefaultTranslogFlushSetting() {
        Settings requestSettings = Settings.builder().put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true).build();

        // This should not throw an exception
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings)
        );
        assertEquals("You can configure 'index.translog.flush_threshold_size' with upto '130mb' for composite index", ex.getMessage());
    }

    public void testMissingCompositeIndexSetting() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "50mb")
            .build();

        // This should not throw an exception
        MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings);
    }

    public void testNullTranslogFlushSetting() {
        Settings requestSettings = Settings.builder()
            .putNull(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey())
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .build();

        // This should not throw an exception
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings)
        );
        assertEquals("You can configure 'index.translog.flush_threshold_size' with upto '130mb' for composite index", ex.getMessage());
    }

    public void testExceedingMaxFlushSize() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "150mb")
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings)
        );
        assertEquals("You can configure 'index.translog.flush_threshold_size' with upto '130mb' for composite index", ex.getMessage());
    }

    public void testEqualToMaxFlushSize() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "100mb")
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .build();

        // This should not throw an exception
        MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(requestSettings, clusterSettings);
    }

    public void testUpdateIndexThresholdFlushSize() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "100mb")
            .build();

        Settings indexSettings = Settings.builder().put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true).build();

        // This should not throw an exception
        assertTrue(
            MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(
                requestSettings,
                clusterSettings,
                indexSettings
            ).isEmpty()
        );
    }

    public void testUpdateFlushSizeAboveThresholdWithCompositeIndex() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "131mb")
            .build();

        Settings indexSettings = Settings.builder().put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true).build();

        Optional<String> err = MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(
            requestSettings,
            clusterSettings,
            indexSettings
        );
        assertTrue(err.isPresent());
        assertEquals("You can configure 'index.translog.flush_threshold_size' with upto '130mb' for composite index", err.get());
    }

    public void testUpdateFlushSizeAboveThresholdWithoutCompositeIndex() {
        Settings requestSettings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "131mb")
            .build();

        Settings indexSettings = Settings.builder().build();

        // This should not throw an exception
        assertTrue(
            MetadataCreateIndexService.validateTranslogFlushIntervalSettingsForCompositeIndex(
                requestSettings,
                clusterSettings,
                indexSettings
            ).isEmpty()
        );
    }
}
