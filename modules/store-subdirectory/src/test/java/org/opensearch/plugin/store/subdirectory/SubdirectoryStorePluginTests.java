/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.subdirectory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreStats;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SubdirectoryStorePluginTests extends OpenSearchTestCase {

    public void testPluginInstantiation() {
        SubdirectoryStorePlugin plugin = new SubdirectoryStorePlugin();
        assertNotNull(plugin);
    }

    public void testGetStoreFactories() {
        SubdirectoryStorePlugin plugin = new SubdirectoryStorePlugin();
        Map<String, IndexStorePlugin.StoreFactory> factories = plugin.getStoreFactories();

        assertNotNull(factories);
        assertTrue(factories.containsKey("subdirectory_store"));

        IndexStorePlugin.StoreFactory factory = factories.get("subdirectory_store");
        assertNotNull(factory);
        assertTrue(factory instanceof SubdirectoryStorePlugin.SubdirectoryStoreFactory);
    }

    public void testStats() throws IOException {
        final ShardId shardId = new ShardId("index", "_na_", 1);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMinutes(0))
            .build();
        Path path = createTempDir().resolve("indices").resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        SubdirectoryAwareStore store = new SubdirectoryAwareStore(
            shardId,
            IndexSettingsModule.newIndexSettings("index", settings),
            SubdirectoryStorePluginTests.newFSDirectory(path.resolve("index")),
            new DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            new ShardPath(false, path, path, shardId),
            new FsDirectoryFactory()
        );

        long initialStoreSize = 0;
        for (String extraFiles : store.directory().listAll()) {
            assertTrue("expected extraFS file but got: " + extraFiles, extraFiles.startsWith("extra"));
            initialStoreSize += store.directory().fileLength(extraFiles);
        }

        final long reservedBytes = randomBoolean() ? StoreStats.UNKNOWN_RESERVED_BYTES : randomLongBetween(0L, Integer.MAX_VALUE);
        StoreStats stats = store.stats(reservedBytes);
        assertEquals(initialStoreSize, stats.getSize().getBytes());
        assertEquals(reservedBytes, stats.getReservedSize().getBytes());

        stats.add(null);
        assertEquals(initialStoreSize, stats.getSize().getBytes());
        assertEquals(reservedBytes, stats.getReservedSize().getBytes());

        final long otherStatsBytes = randomLongBetween(0L, Integer.MAX_VALUE);
        final long otherStatsReservedBytes = randomBoolean() ? StoreStats.UNKNOWN_RESERVED_BYTES : randomLongBetween(0L, Integer.MAX_VALUE);
        stats.add(new StoreStats.Builder().sizeInBytes(otherStatsBytes).reservedSize(otherStatsReservedBytes).build());
        assertEquals(initialStoreSize + otherStatsBytes, stats.getSize().getBytes());
        assertEquals(Math.max(reservedBytes, 0L) + Math.max(otherStatsReservedBytes, 0L), stats.getReservedSize().getBytes());

        Directory dir = store.directory();
        final long length;
        try (IndexOutput output = dir.createOutput("foo/bar", IOContext.DEFAULT)) {
            int iters = scaledRandomIntBetween(10, 100);
            for (int i = 0; i < iters; i++) {
                BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
                output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            }
            length = output.getFilePointer();
        }

        assertTrue(numNonExtraFiles(store) > 0);
        stats = store.stats(0L);
        assertEquals(stats.getSizeInBytes(), length + initialStoreSize);

        deleteContent(store.directory());
        IOUtils.close(store);
    }

    public static void deleteContent(Directory directory) throws IOException {
        final String[] files = directory.listAll();
        final List<IOException> exceptions = new ArrayList<>();
        for (String file : files) {
            try {
                directory.deleteFile(file);
            } catch (NoSuchFileException | FileNotFoundException e) {
                // ignore
            } catch (IOException e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    public int numNonExtraFiles(Store store) throws IOException {
        int numNonExtra = 0;
        for (String file : store.directory().listAll()) {
            if (file.startsWith("extra") == false) {
                numNonExtra++;
            }
        }
        return numNonExtra;
    }
}
