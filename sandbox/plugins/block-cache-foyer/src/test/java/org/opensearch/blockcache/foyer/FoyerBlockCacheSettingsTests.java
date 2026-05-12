/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link FoyerBlockCacheSettings} validation and defaults.
 */
public class FoyerBlockCacheSettingsTests extends OpenSearchTestCase {

    // ── CACHE_SIZE_SETTING ────────────────────────────────────────────────────

    public void testCacheSizeDefault() {
        assertEquals("25%", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(Settings.EMPTY));
    }

    public void testCacheSizeAcceptsPercentage() {
        assertEquals("50%", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
            Settings.builder().put("block_cache.size", "50%").build()));
    }

    public void testCacheSizeAcceptsRatioForm() {
        assertEquals("0.25", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
            Settings.builder().put("block_cache.size", "0.25").build()));
    }

    public void testCacheSizeAcceptsZero() {
        assertEquals("0%", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
            Settings.builder().put("block_cache.size", "0%").build()));
    }

    public void testCacheSizeAcceptsNearMaximum() {
        assertEquals("99%", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
            Settings.builder().put("block_cache.size", "99%").build()));
    }

    public void testCacheSizeRejectsHundredPercent() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
            FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
                Settings.builder().put("block_cache.size", "100%").build()));
        assertTrue(ex.getMessage().contains("block_cache.size"));
    }

    public void testCacheSizeRejectsNegative() {
        expectThrows(IllegalArgumentException.class, () ->
            FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
                Settings.builder().put("block_cache.size", "-1%").build()));
    }

    public void testCacheSizeRejectsRatioOne() {
        expectThrows(IllegalArgumentException.class, () ->
            FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
                Settings.builder().put("block_cache.size", "1.0").build()));
    }

    public void testCacheSizeRejectsGarbage() {
        expectThrows(IllegalArgumentException.class, () ->
            FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(
                Settings.builder().put("block_cache.size", "notanumber").build()));
    }

    // ── IO_ENGINE_SETTING ─────────────────────────────────────────────────────

    public void testIoEngineDefault() {
        assertEquals("auto", FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(Settings.EMPTY));
    }

    public void testIoEngineAcceptsPsync() {
        assertEquals("psync", FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(
            Settings.builder().put("block_cache.io_engine", "psync").build()));
    }

    public void testIoEngineAcceptsIoUring() {
        assertEquals("io_uring", FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(
            Settings.builder().put("block_cache.io_engine", "io_uring").build()));
    }

    public void testIoEngineRejectsUnknown() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
            FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(
                Settings.builder().put("block_cache.io_engine", "libaio").build()));
        assertTrue(ex.getMessage().contains("auto"));
        assertTrue(ex.getMessage().contains("io_uring"));
        assertTrue(ex.getMessage().contains("psync"));
    }

    public void testIoEngineIsCaseSensitive() {
        expectThrows(IllegalArgumentException.class, () ->
            FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(
                Settings.builder().put("block_cache.io_engine", "PSYNC").build()));
    }

    public void testIoEngineRejectsEmpty() {
        expectThrows(IllegalArgumentException.class, () ->
            FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(
                Settings.builder().put("block_cache.io_engine", "").build()));
    }

    // ── BLOCK_SIZE_SETTING ────────────────────────────────────────────────────

    public void testBlockSizeDefault() {
        assertEquals(new ByteSizeValue(64, ByteSizeUnit.MB),
            FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(Settings.EMPTY));
    }

    public void testBlockSizeAcceptsMinimum() {
        assertEquals(new ByteSizeValue(1, ByteSizeUnit.MB),
            FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(
                Settings.builder().put("block_cache.block_size", "1mb").build()));
    }

    public void testBlockSizeAcceptsMaximum() {
        assertEquals(new ByteSizeValue(256, ByteSizeUnit.MB),
            FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(
                Settings.builder().put("block_cache.block_size", "256mb").build()));
    }

    public void testBlockSizeRejectsBelowMinimum() {
        expectThrows(IllegalArgumentException.class, () ->
            FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(
                Settings.builder().put("block_cache.block_size", "512kb").build()));
    }

    public void testBlockSizeRejectsAboveMaximum() {
        expectThrows(IllegalArgumentException.class, () ->
            FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(
                Settings.builder().put("block_cache.block_size", "257mb").build()));
    }

    // ── DATA_TO_CACHE_RATIO_SETTING ───────────────────────────────────────────

    public void testDataToCacheRatioDefault() {
        assertEquals(5.0, FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING.get(Settings.EMPTY), 0.0);
    }

    public void testDataToCacheRatioAcceptsOne() {
        assertEquals(1.0, FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING.get(
            Settings.builder().put("block_cache.data_to_cache_ratio", "1.0").build()), 0.0);
    }

    public void testDataToCacheRatioAcceptsLargeValue() {
        assertEquals(100.0, FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING.get(
            Settings.builder().put("block_cache.data_to_cache_ratio", "100.0").build()), 0.0);
    }

    public void testDataToCacheRatioRejectsBelowOne() {
        expectThrows(IllegalArgumentException.class, () ->
            FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING.get(
                Settings.builder().put("block_cache.data_to_cache_ratio", "0.5").build()));
    }

    public void testDataToCacheRatioRejectsZero() {
        expectThrows(IllegalArgumentException.class, () ->
            FoyerBlockCacheSettings.DATA_TO_CACHE_RATIO_SETTING.get(
                Settings.builder().put("block_cache.data_to_cache_ratio", "0.0").build()));
    }
}
