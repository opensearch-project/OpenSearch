/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.cache;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

/**
 * Unit tests for {@link CacheUtils.CacheType} effective size computation, in particular the
 * warm-node multiplier applied to the metadata cache.
 */
public class CacheUtilsTests extends OpenSearchTestCase {

    private ClusterSettings clusterSettings(Settings settings) {
        return new ClusterSettings(
            settings,
            Set.of(
                CacheSettings.METADATA_CACHE_SIZE_LIMIT,
                CacheSettings.METADATA_CACHE_WARM_SIZE_MULTIPLIER,
                CacheSettings.STATISTICS_CACHE_SIZE_LIMIT
            )
        );
    }

    public void testMetadataNonWarmNodeUsesBaseSize() {
        ClusterSettings cs = clusterSettings(Settings.builder().put(CacheSettings.METADATA_CACHE_SIZE_LIMIT_KEY, "200mb").build());
        long expected = new ByteSizeValue(200, ByteSizeUnit.MB).getBytes();
        assertEquals(expected, CacheUtils.CacheType.METADATA.getEffectiveSizeLimitBytes(cs, false));
    }

    public void testMetadataWarmNodeAppliesMultiplier() {
        ClusterSettings cs = clusterSettings(
            Settings.builder()
                .put(CacheSettings.METADATA_CACHE_SIZE_LIMIT_KEY, "200mb")
                .put(CacheSettings.METADATA_CACHE_WARM_SIZE_MULTIPLIER_KEY, 2.5)
                .build()
        );
        long base = new ByteSizeValue(200, ByteSizeUnit.MB).getBytes();
        assertEquals((long) (base * 2.5), CacheUtils.CacheType.METADATA.getEffectiveSizeLimitBytes(cs, true));
    }

    public void testMetadataWarmNodeWithDefaultMultiplierIsNoOp() {
        ClusterSettings cs = clusterSettings(Settings.builder().put(CacheSettings.METADATA_CACHE_SIZE_LIMIT_KEY, "200mb").build());
        long base = new ByteSizeValue(200, ByteSizeUnit.MB).getBytes();
        // Default multiplier is 1.0, so even on a warm node the size is unchanged.
        assertEquals(base, CacheUtils.CacheType.METADATA.getEffectiveSizeLimitBytes(cs, true));
    }

    public void testStatisticsCacheIgnoresWarmMultiplier() {
        ClusterSettings cs = clusterSettings(
            Settings.builder()
                .put(CacheSettings.STATISTICS_CACHE_SIZE_LIMIT_KEY, "80mb")
                .put(CacheSettings.METADATA_CACHE_WARM_SIZE_MULTIPLIER_KEY, 4.0)
                .build()
        );
        long base = new ByteSizeValue(80, ByteSizeUnit.MB).getBytes();
        assertEquals(base, CacheUtils.CacheType.STATISTICS.getEffectiveSizeLimitBytes(cs, true));
        assertEquals(base, CacheUtils.CacheType.STATISTICS.getEffectiveSizeLimitBytes(cs, false));
    }

    public void testWarmMultiplierClampsOnOverflow() {
        ClusterSettings cs = clusterSettings(
            Settings.builder()
                .put(CacheSettings.METADATA_CACHE_SIZE_LIMIT_KEY, "5000pb")
                .put(CacheSettings.METADATA_CACHE_WARM_SIZE_MULTIPLIER_KEY, 4.0)
                .build()
        );
        // base * 4.0 exceeds Long.MAX_VALUE and must be clamped.
        assertEquals(Long.MAX_VALUE, CacheUtils.CacheType.METADATA.getEffectiveSizeLimitBytes(cs, true));
    }
}
