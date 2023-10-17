/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RemoteStoreFileDownloaderTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Directory source;
    private Directory destination;
    private Directory secondDestination;
    private RemoteStoreFileDownloader fileDownloader;
    private Map<String, Integer> files = new HashMap<>();

    @Before
    public void setup() throws IOException {
        final int streamLimit = randomIntBetween(1, 20);
        final RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder().put("indices.recovery.max_concurrent_remote_store_streams", streamLimit).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        threadPool = new TestThreadPool(getTestName());
        source = new NIOFSDirectory(createTempDir());
        destination = new NIOFSDirectory(createTempDir());
        secondDestination = new NIOFSDirectory(createTempDir());
        for (int i = 0; i < 10; i++) {
            final String filename = "file_" + i;
            final int content = randomInt();
            try (IndexOutput output = source.createOutput(filename, IOContext.DEFAULT)) {
                output.writeInt(content);
            }
            files.put(filename, content);
        }
        fileDownloader = new RemoteStoreFileDownloader(
            ShardId.fromString("[RemoteStoreFileDownloaderTests][0]"),
            threadPool,
            recoverySettings
        );
    }

    @After
    public void stopThreadPool() throws Exception {
        threadPool.shutdown();
        assertTrue(threadPool.awaitTermination(5, TimeUnit.SECONDS));
    }

    public void testDownload() throws IOException {
        fileDownloader.download(source, destination, files.keySet());
        assertContent(files, destination);
    }

    public void testDownloadWithSecondDestination() throws IOException {
        fileDownloader.download(source, destination, secondDestination, files.keySet(), () -> {});
        assertContent(files, destination);
        assertContent(files, secondDestination);
    }

    public void testDownloadWithFileCompletionHandler() throws IOException {
        final AtomicInteger counter = new AtomicInteger(0);
        fileDownloader.download(source, destination, null, files.keySet(), counter::incrementAndGet);
        assertContent(files, destination);
        assertEquals(files.size(), counter.get());
    }

    public void testDownloadNonExistentFile() {
        assertThrows(NoSuchFileException.class, () -> fileDownloader.download(source, destination, Set.of("not real")));
    }

    public void testDownloadExtraNonExistentFile() {
        List<String> filesWithExtra = new ArrayList<>(files.keySet());
        filesWithExtra.add("not real");
        assertThrows(NoSuchFileException.class, () -> fileDownloader.download(source, destination, filesWithExtra));
    }

    private static void assertContent(Map<String, Integer> expected, Directory destination) throws IOException {
        // Note that Lucene will randomly write extra files (see org.apache.lucene.tests.mockfile.ExtraFS)
        // so we just need to check that all the expected files are present but not that _only_ the expected
        // files are present
        final Set<String> actualFiles = Set.of(destination.listAll());
        for (String file : expected.keySet()) {
            assertTrue(actualFiles.contains(file));
            try (IndexInput input = destination.openInput(file, IOContext.DEFAULT)) {
                assertEquals(expected.get(file), Integer.valueOf(input.readInt()));
                assertThrows(EOFException.class, input::readByte);
            }
        }
    }
}
