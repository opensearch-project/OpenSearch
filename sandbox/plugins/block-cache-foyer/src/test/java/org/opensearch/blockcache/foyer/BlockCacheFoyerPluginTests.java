/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.opensearch.blockcache.BlockCacheHandle;
import org.opensearch.blockcache.spi.BlockCacheProvider;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Unit tests for {@link BlockCacheFoyerPlugin}.
 *
 * <p>Focuses on the pure-Java wiring of the plugin:
 * <ul>
 *   <li>Constructor variants (no-arg and {@code Settings}-arg).</li>
 *   <li>{@link BlockCacheFoyerPlugin#loadExtensions(ExtensiblePlugin.ExtensionLoader)}
 *       correctly populates the providers list.</li>
 *   <li>{@link BlockCacheFoyerPlugin#providers()} returns an unmodifiable snapshot.</li>
 *   <li>{@link BlockCacheFoyerPlugin#handle()} returns {@code null} before
 *       {@code createComponents} has run.</li>
 * </ul>
 *
 * <p>Tests that exercise {@code createComponents} are out of scope here because
 * it constructs a real {@link FoyerBlockCache} which requires the native library.
 * Those paths are covered by integration tests.
 */
public class BlockCacheFoyerPluginTests extends OpenSearchTestCase {

    public void testNoArgConstructor() {
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        assertNotNull(plugin);
        assertEquals("no providers discovered yet", 0, plugin.providers().size());
        assertNull("handle is null before createComponents", plugin.handle());
    }

    public void testSettingsConstructor() {
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin(Settings.EMPTY);
        assertNotNull(plugin);
        assertEquals(0, plugin.providers().size());
    }

    public void testLoadExtensionsPopulatesProviders() {
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        final RecordingProvider p1 = new RecordingProvider("s3");
        final RecordingProvider p2 = new RecordingProvider("gcs");

        plugin.loadExtensions(stubLoaderWith(p1, p2));

        assertEquals(2, plugin.providers().size());
        assertTrue(plugin.providers().contains(p1));
        assertTrue(plugin.providers().contains(p2));
    }

    public void testLoadExtensionsWithEmptyList() {
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        plugin.loadExtensions(stubLoaderWith(/* none */));
        assertEquals(0, plugin.providers().size());
    }

    public void testProvidersReturnsUnmodifiableSnapshot() {
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        plugin.loadExtensions(stubLoaderWith(new RecordingProvider("s3")));

        final List<BlockCacheProvider> snapshot = plugin.providers();
        assertEquals(1, snapshot.size());

        expectThrows(UnsupportedOperationException.class, () -> snapshot.add(new RecordingProvider("gcs")));
    }

    public void testProvidersSnapshotDecoupledFromInternalList() {
        // A second loadExtensions call appends to the internal list, but a previously-returned
        // snapshot must not see the new additions.
        final BlockCacheFoyerPlugin plugin = new BlockCacheFoyerPlugin();
        plugin.loadExtensions(stubLoaderWith(new RecordingProvider("s3")));
        final List<BlockCacheProvider> firstSnapshot = plugin.providers();

        plugin.loadExtensions(stubLoaderWith(new RecordingProvider("gcs")));
        final List<BlockCacheProvider> secondSnapshot = plugin.providers();

        assertEquals("first snapshot is fixed at 1 element", 1, firstSnapshot.size());
        assertEquals("second snapshot reflects both discoveries", 2, secondSnapshot.size());
    }

    // ── Test helpers ──────────────────────────────────────────────────────────

    /**
     * Minimal {@link ExtensiblePlugin.ExtensionLoader} stub that returns a fixed list
     * of {@link BlockCacheProvider} instances for {@code loadExtensions(BlockCacheProvider.class)}
     * and an empty list for every other extension type.
     */
    private static ExtensiblePlugin.ExtensionLoader stubLoaderWith(BlockCacheProvider... providers) {
        final List<BlockCacheProvider> list = List.of(providers);
        return new ExtensiblePlugin.ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked")
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                if (extensionPointType == BlockCacheProvider.class) {
                    return (List<T>) list;
                }
                return List.of();
            }
        };
    }

    /** Provider test double that records each {@link #attach} invocation. */
    private static final class RecordingProvider implements BlockCacheProvider {
        private final String type;
        private final Collection<RepositoryMetadata> attached = new ArrayList<>();

        RecordingProvider(String type) {
            this.type = type;
        }

        @Override
        public String repositoryType() {
            return type;
        }

        @Override
        public void attach(BlockCacheHandle handle, RepositoryMetadata metadata) {
            attached.add(metadata);
        }

        int attachCount() {
            return attached.size();
        }
    }
}
