/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.regex;

import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RegexpAutomatonCacheTests extends OpenSearchTestCase {

    private ClusterService createClusterService(Settings settings) {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return clusterService;
    }

    public void testCacheDisabledByDefault() {
        Settings settings = Settings.EMPTY;
        ClusterService clusterService = createClusterService(settings);
        try (RegexpAutomatonCache cache = new RegexpAutomatonCache(settings, clusterService)) {
            assertFalse(cache.isEnabled());
            assertEquals(0, cache.hits());
            assertEquals(0, cache.misses());
            assertEquals(0, cache.evictions());

            CompiledAutomaton c1 = cache.getCompiledAutomaton("abc.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            CompiledAutomaton c2 = cache.getCompiledAutomaton("abc.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            assertNotNull(c1);
            assertNotNull(c2);
            assertNotSame(c1, c2);
            assertEquals(0, cache.hits());
            assertEquals(0, cache.misses());
        }
    }

    public void testCacheHitAndMiss() {
        Settings settings = Settings.builder().put(RegexpAutomatonCache.SETTING_AUTOMATON_CACHE_ENABLED.getKey(), true).build();
        ClusterService clusterService = createClusterService(settings);
        try (RegexpAutomatonCache cache = new RegexpAutomatonCache(settings, clusterService)) {
            assertTrue(cache.isEnabled());
            assertEquals(0, cache.hits());
            assertEquals(0, cache.misses());

            CompiledAutomaton c1 = cache.getCompiledAutomaton("test.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            assertEquals(0, cache.hits());
            assertEquals(1, cache.misses());

            CompiledAutomaton c2 = cache.getCompiledAutomaton("test.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            assertEquals(1, cache.hits());
            assertEquals(1, cache.misses());
            assertSame(c1, c2);

            // Different pattern should miss
            CompiledAutomaton c3 = cache.getCompiledAutomaton("other.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            assertEquals(1, cache.hits());
            assertEquals(2, cache.misses());
            assertNotSame(c1, c3);
        }
    }

    public void testEvictionAndMetrics() {
        // Small cache size to trigger evictions
        Settings settings = Settings.builder()
            .put(RegexpAutomatonCache.SETTING_AUTOMATON_CACHE_ENABLED.getKey(), true)
            .put(RegexpAutomatonCache.SETTING_AUTOMATON_CACHE_MAX_SIZE.getKey(), "500b")
            .build();
        ClusterService clusterService = createClusterService(settings);
        try (RegexpAutomatonCache cache = new RegexpAutomatonCache(settings, clusterService)) {
            assertTrue(cache.isEnabled());

            // Add several different complex regexes to exceed 500 bytes RAM
            for (int i = 0; i < 50; i++) {
                cache.getCompiledAutomaton("pattern_" + i + ".*[a-z]+", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            }

            assertTrue("Evictions should occur when max_size_bytes is tiny", cache.evictions() > 0);
            assertEquals(50, cache.misses());
        }
    }

    public void testWarmResize() {
        Settings settings = Settings.builder()
            .put(RegexpAutomatonCache.SETTING_AUTOMATON_CACHE_ENABLED.getKey(), true)
            .put(RegexpAutomatonCache.SETTING_AUTOMATON_CACHE_MAX_SIZE.getKey(), "10mb")
            .build();
        ClusterService clusterService = createClusterService(settings);
        try (RegexpAutomatonCache cache = new RegexpAutomatonCache(settings, clusterService)) {
            CompiledAutomaton c1 = cache.getCompiledAutomaton("pattern1.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            CompiledAutomaton c2 = cache.getCompiledAutomaton("pattern2.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            assertEquals(0, cache.hits());
            assertEquals(2, cache.misses());

            // Trigger warm resize by updating max size
            cache.setMaxSize(ByteSizeValue.parseBytesSizeValue("20mb", RegexpAutomatonCache.SETTING_AUTOMATON_CACHE_MAX_SIZE.getKey()));
            assertEquals(20 * 1024 * 1024, cache.getMaxSizeBytes());

            // Check if pattern1 is still cached after resize
            CompiledAutomaton c1After = cache.getCompiledAutomaton("pattern1.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            assertSame(c1, c1After);
            assertEquals(1, cache.hits());
            assertEquals(2, cache.misses());
        }
    }

    public void testEnableDisableToggling() {
        Settings settings = Settings.EMPTY;
        ClusterService clusterService = createClusterService(settings);
        try (RegexpAutomatonCache cache = new RegexpAutomatonCache(settings, clusterService)) {
            assertFalse(cache.isEnabled());

            cache.setEnabled(true);
            assertTrue(cache.isEnabled());

            CompiledAutomaton c1 = cache.getCompiledAutomaton("regex.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            assertEquals(1, cache.misses());

            CompiledAutomaton c2 = cache.getCompiledAutomaton("regex.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            assertSame(c1, c2);
            assertEquals(1, cache.hits());

            cache.setEnabled(false);
            assertFalse(cache.isEnabled());
            CompiledAutomaton c3 = cache.getCompiledAutomaton("regex.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
            assertNotSame(c1, c3);
        }
    }
}
