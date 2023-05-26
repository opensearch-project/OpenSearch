/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.junit.Assert;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.indices.recovery.MultiChunkTransfer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.doNothing;

public class SegmentFileTransferHandlerTests extends IndexShardTestCase {

    private IndexShard shard;
    private StoreFileMetadata[] filesToSend;
    private final DiscoveryNode targetNode = new DiscoveryNode(
        "foo",
        buildNewFakeTransportAddress(),
        emptyMap(),
        emptySet(),
        Version.CURRENT
    );

    final int fileChunkSizeInBytes = 5000;
    final int maxConcurrentFileChunks = 1;
    private CancellableThreads cancellableThreads;
    final IntSupplier translogOps = () -> 0;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        cancellableThreads = new CancellableThreads();
        shard = spy(newStartedShard(true));
        filesToSend = getFilestoSend(shard);
        // we should only have a Segments_N file at this point.
        assertEquals(1, filesToSend.length);
    }

    private StoreFileMetadata[] getFilestoSend(IndexShard shard) throws IOException {
        final Store.MetadataSnapshot metadata = shard.store().getMetadata();
        return metadata.asMap().values().toArray(StoreFileMetadata[]::new);
    }

    @Override
    public void tearDown() throws Exception {
        closeShards(shard);
        super.tearDown();
    }

    public void testSendFiles_invokesChunkWriter() throws IOException, InterruptedException {
        // use counDownLatch and countDown when our chunkWriter is invoked.
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final FileChunkWriter chunkWriter = spy(new FileChunkWriter() {
            @Override
            public void writeFileChunk(
                StoreFileMetadata fileMetadata,
                long position,
                BytesReference content,
                boolean lastChunk,
                int totalTranslogOps,
                ActionListener<Void> listener
            ) {
                assertTrue(filesToSend[0].isSame(fileMetadata));
                assertTrue(lastChunk);
                countDownLatch.countDown();
            }
        });

        SegmentFileTransferHandler handler = new SegmentFileTransferHandler(
            shard,
            targetNode,
            chunkWriter,
            logger,
            shard.getThreadPool(),
            cancellableThreads,
            fileChunkSizeInBytes,
            maxConcurrentFileChunks
        );
        final MultiChunkTransfer<StoreFileMetadata, SegmentFileTransferHandler.FileChunk> transfer = handler.createTransfer(
            shard.store(),
            filesToSend,
            translogOps,
            mock(ActionListener.class)
        );

        // start the transfer
        transfer.start();
        countDownLatch.await(5, TimeUnit.SECONDS);
        verify(chunkWriter, times(1)).writeFileChunk(any(), anyLong(), any(), anyBoolean(), anyInt(), any());
        IOUtils.close(transfer);
    }

    public void testSendFiles_cancelThreads_beforeStart() throws IOException, InterruptedException {
        final FileChunkWriter chunkWriter = spy(new FileChunkWriter() {
            @Override
            public void writeFileChunk(
                StoreFileMetadata fileMetadata,
                long position,
                BytesReference content,
                boolean lastChunk,
                int totalTranslogOps,
                ActionListener<Void> listener
            ) {
                Assert.fail();
            }
        });
        SegmentFileTransferHandler handler = new SegmentFileTransferHandler(
            shard,
            targetNode,
            chunkWriter,
            logger,
            shard.getThreadPool(),
            cancellableThreads,
            fileChunkSizeInBytes,
            maxConcurrentFileChunks
        );

        final MultiChunkTransfer<StoreFileMetadata, SegmentFileTransferHandler.FileChunk> transfer = handler.createTransfer(
            shard.store(),
            filesToSend,
            translogOps,
            mock(ActionListener.class)
        );

        // start the transfer
        cancellableThreads.cancel("test");
        transfer.start();
        verifyNoInteractions(chunkWriter);
        IOUtils.close(transfer);
    }

    public void testSendFiles_cancelThreads_afterStart() throws IOException, InterruptedException {
        // index a doc a flush so we have more than 1 file to send.
        indexDoc(shard, "_doc", "test");
        flushShard(shard, true);
        filesToSend = getFilestoSend(shard);

        // we should have 4 files to send now -
        // [_0.cfe, _0.si, _0.cfs, segments_3]
        assertEquals(4, filesToSend.length);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        FileChunkWriter chunkWriter = spy(new FileChunkWriter() {
            @Override
            public void writeFileChunk(
                StoreFileMetadata fileMetadata,
                long position,
                BytesReference content,
                boolean lastChunk,
                int totalTranslogOps,
                ActionListener<Void> listener
            ) {
                // cancel the threads at this point, we'll ensure this is not invoked more than once.
                cancellableThreads.cancel("test");
                listener.onResponse(null);
            }
        });
        SegmentFileTransferHandler handler = new SegmentFileTransferHandler(
            shard,
            targetNode,
            chunkWriter,
            logger,
            shard.getThreadPool(),
            cancellableThreads,
            fileChunkSizeInBytes,
            maxConcurrentFileChunks
        );

        final MultiChunkTransfer<StoreFileMetadata, SegmentFileTransferHandler.FileChunk> transfer = handler.createTransfer(
            shard.store(),
            filesToSend,
            translogOps,
            new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    // do nothing here, we will just resolve in test.
                }

                @Override
                public void onFailure(Exception e) {
                    assertEquals(CancellableThreads.ExecutionCancelledException.class, e.getClass());
                    countDownLatch.countDown();
                }
            }
        );

        // start the transfer
        transfer.start();
        countDownLatch.await(30, TimeUnit.SECONDS);
        verify(chunkWriter, times(1)).writeFileChunk(any(), anyLong(), any(), anyBoolean(), anyInt(), any());
        IOUtils.close(transfer);
    }

    public void testSendFiles_CorruptIndexException() throws Exception {
        final CancellableThreads cancellableThreads = new CancellableThreads();
        SegmentFileTransferHandler handler = new SegmentFileTransferHandler(
            shard,
            targetNode,
            mock(FileChunkWriter.class),
            logger,
            shard.getThreadPool(),
            cancellableThreads,
            fileChunkSizeInBytes,
            maxConcurrentFileChunks
        );
        final StoreFileMetadata SEGMENTS_FILE = new StoreFileMetadata(
            IndexFileNames.SEGMENTS,
            1L,
            "0",
            org.apache.lucene.util.Version.LATEST
        );

        doNothing().when(shard).failShard(anyString(), any());
        assertThrows(CorruptIndexException.class, () -> {
            handler.handleErrorOnSendFiles(
                shard.store(),
                new CorruptIndexException("test", "test"),
                new StoreFileMetadata[] { SEGMENTS_FILE }
            );
        });

        verify(shard, times(1)).failShard(any(), any());
    }
}
