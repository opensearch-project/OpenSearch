/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.PathUtilsForTesting;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_MODE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies on a running cluster that a segments_only translog, which is never uploaded anywhere, is still fsynced
 * before a write is acknowledged.
 *
 * <p>Stopping a node cannot show this. The operating system page cache outlives the process, so the data is readable
 * afterwards whether or not it was ever forced to disk, and only the loss of the machine would tell the two apart.
 * This test instead counts the {@code force} calls the shard makes on its own translog files, following the
 * filesystem interception that {@code DiskDisruptionIT} uses to simulate a power outage.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class SegmentsOnlyTranslogFsyncIT extends OpenSearchIntegTestCase {

    private static final String SEGMENT_REPOSITORY_NAME = "test-segment-repo";
    private static final String INDEX_NAME = "fsync-idx";

    private static FsyncCountingFileSystemProvider fileSystemProvider;

    private Path segmentRepoPath;

    @BeforeClass
    public static void installFsyncCountingFileSystem() {
        fileSystemProvider = new FsyncCountingFileSystemProvider(PathUtils.getDefaultFileSystem());
        PathUtilsForTesting.installMock(fileSystemProvider.getFileSystem(null));
    }

    @AfterClass
    public static void removeFsyncCountingFileSystem() {
        PathUtilsForTesting.teardown();
        fileSystemProvider = null;
    }

    /** Counts {@code force} calls on translog files, which is the syscall that makes an operation survive the machine. */
    static class FsyncCountingFileSystemProvider extends FilterFileSystemProvider {

        final AtomicBoolean counting = new AtomicBoolean();
        final AtomicInteger translogFsyncs = new AtomicInteger();
        final AtomicInteger checkpointFsyncs = new AtomicInteger();

        FsyncCountingFileSystemProvider(FileSystem inner) {
            super("fsynccounting://", inner);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            FileChannel delegate = super.newFileChannel(path, options, attrs);
            String name = path.getFileName().toString();
            if (name.endsWith(".tlog") == false && name.endsWith(".ckp") == false) {
                return delegate;
            }
            boolean translog = name.endsWith(".tlog");
            return new FilterFileChannel(delegate) {
                @Override
                public void force(boolean metaData) throws IOException {
                    if (counting.get()) {
                        (translog ? translogFsyncs : checkpointFsyncs).incrementAndGet();
                    }
                    super.force(metaData);
                }
            };
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (segmentRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
        }
        String typeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            SEGMENT_REPOSITORY_NAME
        );
        String settingsPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            SEGMENT_REPOSITORY_NAME
        );
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("node.attr." + REMOTE_STORE_MODE_KEY, "segments_only")
            .put("node.attr." + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, SEGMENT_REPOSITORY_NAME)
            .put(typeKey, ReloadableFsRepository.TYPE)
            .put(settingsPrefix + "location", segmentRepoPath)
            .build();
    }

    public void testLocalTranslogIsFsyncedBeforeWritesAreAcknowledged() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            )
        );
        ensureGreen(INDEX_NAME);

        // Count only the writes themselves, so that shard startup and recovery cannot contribute.
        fileSystemProvider.translogFsyncs.set(0);
        fileSystemProvider.checkpointFsyncs.set(0);
        fileSystemProvider.counting.set(true);
        int docs = randomIntBetween(10, 20);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        fileSystemProvider.counting.set(false);

        logger.info(
            "--- segments_only writes: {} translog fsyncs, {} checkpoint fsyncs over {} docs",
            fileSystemProvider.translogFsyncs.get(),
            fileSystemProvider.checkpointFsyncs.get(),
            docs
        );

        // Before the fix both counters stay at zero: the shard believed its translog was remote backed and skipped
        // the force entirely, so an acknowledged write lived only in the page cache.
        assertThat(
            "a segments_only translog is the only durable copy and must be fsynced",
            fileSystemProvider.translogFsyncs.get(),
            greaterThan(0)
        );
        assertThat("the translog checkpoint must be fsynced alongside it", fileSystemProvider.checkpointFsyncs.get(), greaterThan(0));
    }
}
