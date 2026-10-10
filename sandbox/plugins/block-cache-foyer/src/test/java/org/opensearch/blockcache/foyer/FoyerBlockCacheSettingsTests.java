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
        assertEquals("50%", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(Settings.EMPTY));
    }

    public void testCacheSizeAcceptsPercentage() {
        assertEquals(
            "50%",
            FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.size", "50%").build())
        );
    }

    public void testCacheSizeAcceptsRatioForm() {
        assertEquals(
            "0.25",
            FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.size", "0.25").build())
        );
    }

    public void testCacheSizeAcceptsZero() {
        assertEquals("0%", FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.size", "0%").build()));
    }

    public void testCacheSizeAcceptsNearMaximum() {
        assertEquals(
            "99%",
            FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.size", "99%").build())
        );
    }

    public void testCacheSizeRejectsHundredPercent() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.size", "100%").build())
        );
        assertTrue(ex.getMessage().contains("block_cache.foyer.size"));
    }

    public void testCacheSizeRejectsNegative() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.size", "-1%").build())
        );
    }

    public void testCacheSizeRejectsRatioOne() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.size", "1.0").build())
        );
    }

    public void testCacheSizeRejectsGarbage() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.CACHE_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.size", "notanumber").build())
        );
    }

    // ── IO_ENGINE_SETTING ─────────────────────────────────────────────────────

    public void testIoEngineDefault() {
        assertEquals("psync", FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(Settings.EMPTY));
    }

    public void testIoEngineAcceptsAuto() {
        assertEquals(
            "auto",
            FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(Settings.builder().put("block_cache.foyer.io_engine", "auto").build())
        );
    }

    public void testIoEngineAcceptsPsync() {
        assertEquals(
            "psync",
            FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(Settings.builder().put("block_cache.foyer.io_engine", "psync").build())
        );
    }

    public void testIoEngineAcceptsIoUring() {
        assertEquals(
            "io_uring",
            FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(Settings.builder().put("block_cache.foyer.io_engine", "io_uring").build())
        );
    }

    public void testIoEngineRejectsUnknown() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(Settings.builder().put("block_cache.foyer.io_engine", "libaio").build())
        );
        assertTrue(ex.getMessage().contains("auto"));
        assertTrue(ex.getMessage().contains("io_uring"));
        assertTrue(ex.getMessage().contains("psync"));
    }

    public void testIoEngineIsCaseSensitive() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(Settings.builder().put("block_cache.foyer.io_engine", "PSYNC").build())
        );
    }

    public void testIoEngineRejectsEmpty() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.IO_ENGINE_SETTING.get(Settings.builder().put("block_cache.foyer.io_engine", "").build())
        );
    }

    // ── BLOCK_SIZE_SETTING ────────────────────────────────────────────────────

    public void testBlockSizeDefault() {
        assertEquals(new ByteSizeValue(128, ByteSizeUnit.MB), FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(Settings.EMPTY));
    }

    public void testBlockSizeAcceptsMinimum() {
        assertEquals(
            new ByteSizeValue(1, ByteSizeUnit.MB),
            FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.block_size", "1mb").build())
        );
    }

    public void testBlockSizeAcceptsMaximum() {
        assertEquals(
            new ByteSizeValue(512, ByteSizeUnit.MB),
            FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.block_size", "512mb").build())
        );
    }

    public void testBlockSizeRejectsBelowMinimum() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.block_size", "512kb").build())
        );
    }

    public void testBlockSizeRejectsAboveMaximum() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.BLOCK_SIZE_SETTING.get(Settings.builder().put("block_cache.foyer.block_size", "513mb").build())
        );
    }

    // ── KEY_INDEX_SWEEP_THRESHOLD_SETTING ─────────────────────────────────────

    public void testSweepThresholdDefaultIs0dot70() {
        assertEquals(0.70, FoyerBlockCacheSettings.KEY_INDEX_SWEEP_THRESHOLD_SETTING.get(Settings.EMPTY), 0.001);
    }

    public void testSweepThresholdZeroCanBeSetExplicitly() {
        // 0.0 = always sweep (no threshold guard)
        Settings s = Settings.builder().put("block_cache.foyer.key_index_sweep_threshold", 0.0).build();
        assertEquals(0.0, FoyerBlockCacheSettings.KEY_INDEX_SWEEP_THRESHOLD_SETTING.get(s), 0.0);
    }

    public void testSweepThresholdAccepts0dot5() {
        Settings s = Settings.builder().put("block_cache.foyer.key_index_sweep_threshold", 0.5).build();
        assertEquals(0.5, FoyerBlockCacheSettings.KEY_INDEX_SWEEP_THRESHOLD_SETTING.get(s), 0.001);
    }

    public void testSweepThresholdAccepts1dot0() {
        // 1.0 = only sweep when cache is 100% full; valid boundary
        Settings s = Settings.builder().put("block_cache.foyer.key_index_sweep_threshold", 1.0).build();
        assertEquals(1.0, FoyerBlockCacheSettings.KEY_INDEX_SWEEP_THRESHOLD_SETTING.get(s), 0.0);
    }

    public void testSweepThresholdRejectsNegative() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.KEY_INDEX_SWEEP_THRESHOLD_SETTING.get(
                Settings.builder().put("block_cache.foyer.key_index_sweep_threshold", -0.01).build()
            )
        );
    }

    public void testSweepThresholdRejectsAboveOne() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.KEY_INDEX_SWEEP_THRESHOLD_SETTING.get(
                Settings.builder().put("block_cache.foyer.key_index_sweep_threshold", 1.01).build()
            )
        );
    }

    // ── METADATA_CACHE_RATIO_SETTING ─────────────────────────────────────────

    public void testMetadataCacheRatioDefault() {
        assertEquals("5%", FoyerBlockCacheSettings.METADATA_CACHE_RATIO_SETTING.get(Settings.EMPTY));
    }

    public void testMetadataCacheRatioAcceptsPercentage() {
        assertEquals(
            "10%",
            FoyerBlockCacheSettings.METADATA_CACHE_RATIO_SETTING.get(
                Settings.builder().put("block_cache.foyer.metadata_cache_ratio", "10%").build()
            )
        );
    }

    public void testMetadataCacheRatioAcceptsRatioForm() {
        assertEquals(
            "0.05",
            FoyerBlockCacheSettings.METADATA_CACHE_RATIO_SETTING.get(
                Settings.builder().put("block_cache.foyer.metadata_cache_ratio", "0.05").build()
            )
        );
    }

    public void testMetadataCacheRatioAcceptsZero() {
        assertEquals(
            "0%",
            FoyerBlockCacheSettings.METADATA_CACHE_RATIO_SETTING.get(
                Settings.builder().put("block_cache.foyer.metadata_cache_ratio", "0%").build()
            )
        );
    }

    public void testMetadataCacheRatioAcceptsNearMaximum() {
        assertEquals(
            "49%",
            FoyerBlockCacheSettings.METADATA_CACHE_RATIO_SETTING.get(
                Settings.builder().put("block_cache.foyer.metadata_cache_ratio", "49%").build()
            )
        );
    }

    public void testMetadataCacheRatioRejectsFiftyPercent() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.METADATA_CACHE_RATIO_SETTING.get(
                Settings.builder().put("block_cache.foyer.metadata_cache_ratio", "50%").build()
            )
        );
        assertTrue(ex.getMessage().contains("block_cache.foyer.metadata_cache_ratio"));
    }

    public void testMetadataCacheRatioRejectsNegative() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.METADATA_CACHE_RATIO_SETTING.get(
                Settings.builder().put("block_cache.foyer.metadata_cache_ratio", "-1%").build()
            )
        );
    }

    public void testMetadataCacheRatioRejectsGarbage() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.METADATA_CACHE_RATIO_SETTING.get(
                Settings.builder().put("block_cache.foyer.metadata_cache_ratio", "notanumber").build()
            )
        );
    }

    // ── METADATA_BLOCK_SIZE_SETTING ──────────────────────────────────────────

    public void testMetadataBlockSizeDefault() {
        assertEquals(new ByteSizeValue(8, ByteSizeUnit.MB), FoyerBlockCacheSettings.METADATA_BLOCK_SIZE_SETTING.get(Settings.EMPTY));
    }

    public void testMetadataBlockSizeAcceptsMinimum() {
        assertEquals(
            new ByteSizeValue(1, ByteSizeUnit.MB),
            FoyerBlockCacheSettings.METADATA_BLOCK_SIZE_SETTING.get(
                Settings.builder().put("block_cache.foyer.metadata_block_size", "1mb").build()
            )
        );
    }

    public void testMetadataBlockSizeAcceptsMaximum() {
        assertEquals(
            new ByteSizeValue(128, ByteSizeUnit.MB),
            FoyerBlockCacheSettings.METADATA_BLOCK_SIZE_SETTING.get(
                Settings.builder().put("block_cache.foyer.metadata_block_size", "128mb").build()
            )
        );
    }

    public void testMetadataBlockSizeRejectsBelowMinimum() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.METADATA_BLOCK_SIZE_SETTING.get(
                Settings.builder().put("block_cache.foyer.metadata_block_size", "512kb").build()
            )
        );
    }

    public void testMetadataBlockSizeRejectsAboveMaximum() {
        expectThrows(
            IllegalArgumentException.class,
            () -> FoyerBlockCacheSettings.METADATA_BLOCK_SIZE_SETTING.get(
                Settings.builder().put("block_cache.foyer.metadata_block_size", "129mb").build()
            )
        );
    }

}
