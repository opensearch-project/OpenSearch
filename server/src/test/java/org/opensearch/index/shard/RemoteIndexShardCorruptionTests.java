/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.translog.TestTranslog;
import org.opensearch.index.translog.TranslogCorruptedException;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.CorruptionUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

@LuceneTestCase.SuppressFileSystems("WindowsFS")
public class RemoteIndexShardCorruptionTests extends IndexShardTestCase {

    public void testLocalDirectoryContains() throws IOException {
        IndexShard indexShard = newStartedShard(true);
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(indexShard, "_doc", Integer.toString(i));
        }
        flushShard(indexShard);
        indexShard.store().incRef();
        Directory localDirectory = indexShard.store().directory();
        Path shardPath = indexShard.shardPath().getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);
        Path tempDir = createTempDir();
        for (String file : localDirectory.listAll()) {
            if (file.equals("write.lock") || file.startsWith("extra")) {
                continue;
            }
            boolean corrupted = randomBoolean();
            long checksum = 0;
            try (IndexInput indexInput = localDirectory.openInput(file, IOContext.READONCE)) {
                checksum = CodecUtil.retrieveChecksum(indexInput);
            }
            if (corrupted) {
                Files.copy(shardPath.resolve(file), tempDir.resolve(file));
                try (FileChannel raf = FileChannel.open(shardPath.resolve(file), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    CorruptionUtils.corruptAt(shardPath.resolve(file), raf, (int) (raf.size() - 8));
                }
            }
            if (corrupted == false) {
                assertTrue(indexShard.localDirectoryContains(localDirectory, file, checksum));
            } else {
                assertFalse(indexShard.localDirectoryContains(localDirectory, file, checksum));
                assertFalse(Files.exists(shardPath.resolve(file)));
            }
        }
        try (Stream<Path> files = Files.list(tempDir)) {
            files.forEach(p -> {
                try {
                    Files.copy(p, shardPath.resolve(p.getFileName()));
                } catch (IOException e) {
                    // Ignore
                }
            });
        }
        FileSystemUtils.deleteSubDirectories(tempDir);
        indexShard.store().decRef();
        closeShards(indexShard);
    }

    /**
     * A local translog generation whose operations have rotted but whose footer is intact is reused by the incremental
     * download, so replay is the first thing that notices. The shard must fail that recovery, discard the local
     * translog, and recover cleanly from the remote store on the next attempt instead of reusing the same bytes forever.
     */
    public void testCorruptLocalTranslogIsDiscardedAndRedownloadedFromRemote() throws Exception {
        String remoteStorePath = createTempDir().toString();
        IndexShard shard = newStartedShard(
            true,
            Settings.builder()
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
                .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, remoteStorePath + "__test")
                .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, remoteStorePath + "__test")
                .build(),
            new EngineBackedIndexerFactory(new InternalEngineFactory())
        );
        // Let the segment upload that the start-up refresh scheduled finish first, and then never refresh again: the
        // operations must exist only in the translog, otherwise the commit the recovery downloads already covers them
        // and replay has nothing to read.
        assertBusy(() -> assertTrue(shard.isRemoteSegmentStoreInSync()));
        int numDocs = between(2, 8);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(shard, "_doc", Integer.toString(i));
        }
        Path translogLocation = shard.shardPath().resolveTranslog();
        // Not closeShard(): the harness refreshes the engine on close, and on a remote-store shard that refresh
        // uploads a segment covering the operations, which would leave the translog nothing to replay.
        IOUtils.close(() -> shard.close("test", false, false), shard.store());

        // Rot one operation of a generation the remote store holds, leaving its checkpoint and footer intact.
        corruptOneOperationOfSomeGeneration(translogLocation);

        // The failed replay fails the engine; that is the expected outcome here, not a test failure.
        allowShardFailures();
        IndexShard failing = reinitShard(shard);
        Exception failure = expectThrows(Exception.class, () -> recoverShardFromStore(failing));
        assertNotNull(
            "recovery must fail on the corrupt operation, got " + failure,
            ExceptionsHelper.unwrap(failure, TranslogCorruptedException.class)
        );
        assertFalse("the corrupt local translog must have been discarded", Files.exists(translogLocation));
        failOnShardFailures();

        IndexShard recovered = reinitShard(failing);
        recoverShardFromStore(recovered);
        assertDocCount(recovered, numDocs);
        assertTrue(Files.exists(translogLocation));
        closeShards(recovered);
    }

    /**
     * Without a remote translog the local copy is the only copy: a corrupt generation still fails the recovery, but the
     * shard must not delete anything.
     */
    public void testCorruptLocalTranslogWithoutRemoteStoreIsLeftInPlace() throws Exception {
        IndexShard shard = newStartedShard(true);
        int numDocs = between(2, 8);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(shard, "_doc", Integer.toString(i));
        }
        Path translogLocation = shard.shardPath().resolveTranslog();
        closeShards(shard);

        Path corrupted = corruptOneOperationOfSomeGeneration(translogLocation);

        allowShardFailures();
        IndexShard failing = reinitShard(shard);
        Exception failure = expectThrows(Exception.class, () -> recoverShardFromStore(failing));
        assertNotNull(
            "recovery must fail on the corrupt operation, got " + failure,
            ExceptionsHelper.unwrap(failure, TranslogCorruptedException.class)
        );
        assertTrue(Files.exists(translogLocation));
        assertTrue(Files.exists(corrupted));
        closeShards(failing);
    }

    /**
     * Flips a byte inside the body of the last operation of a randomly chosen non-empty generation. The size prefix,
     * the checkpoint and any footer are untouched, so the generation still looks current and the rot surfaces as a
     * per-operation checksum failure at replay.
     */
    private Path corruptOneOperationOfSomeGeneration(Path translogLocation) throws IOException {
        return TestTranslog.corruptLastOperationOfRandomGeneration(random(), translogLocation);
    }
}
