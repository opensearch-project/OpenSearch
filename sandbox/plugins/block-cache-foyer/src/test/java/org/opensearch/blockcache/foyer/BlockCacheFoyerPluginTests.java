/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link BlockCacheFoyerPlugin}.
 *
 * <p>Focuses on the pure-Java wiring of the plugin that does not require the
 * native library:
 * <ul>
 *   <li>Both constructor variants (no-arg and {@code Settings}-arg).</li>
 *   <li>{@link BlockCacheFoyerPlugin#getBlockCache()} returns
 *       {@code Optional.empty()} before {@code createComponents} has run.</li>
 * </ul>
 *
 * <p>Tests that exercise {@code createComponents} are out of scope here because
 * it constructs a real {@link FoyerBlockCache} which requires the native
 * library. Those paths are covered by integration tests.
 */
public class BlockCacheFoyerPluginTests extends OpenSearchTestCase {

    public void testNoArgConstructor() {
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertNotNull(plugin);
        assertTrue("handle is empty before createComponents", plugin.getBlockCache().isEmpty());
    }

    public void testSettingsConstructor() {
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(Settings.EMPTY);
        assertNotNull(plugin);
        assertTrue(plugin.getBlockCache().isEmpty());
    }
}
