/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.mockito.Mockito;
import org.opensearch.action.ActionListener;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.listener.FileTransferListener;
import org.opensearch.index.translog.transfer.listener.TranslogTransferListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.index.translog.transfer.FileSnapshot.CheckpointFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TranslogFileSnapshot;
import org.opensearch.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

@LuceneTestCase.SuppressFileSystems("*")
public class TranslogTransferManagerTests extends OpenSearchTestCase {

    private TransferService transferService;
    private BlobPath remoteBaseTransferPath;
    private long primaryTerm;
    private long generation;
    private long minTranslogGeneration;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        primaryTerm = randomNonNegativeLong();
        generation = randomNonNegativeLong();
        minTranslogGeneration = randomLongBetween(0, generation);
        remoteBaseTransferPath = new BlobPath().add("base_path");
        transferService = mock(TransferService.class);
    }

    @SuppressWarnings("unchecked")
    public void testTransferSnapshot() throws IOException {
        AtomicInteger fileTransferSucceeded = new AtomicInteger();
        AtomicInteger fileTransferFailed = new AtomicInteger();
        AtomicInteger translogTransferSucceeded = new AtomicInteger();
        AtomicInteger translogTransferFailed = new AtomicInteger();

        doNothing().when(transferService)
            .uploadBlob(any(TransferFileSnapshot.class), Mockito.eq(remoteBaseTransferPath.add(String.valueOf(primaryTerm))));
        doAnswer(invocationOnMock -> {
            ActionListener<TransferFileSnapshot> listener = (ActionListener<TransferFileSnapshot>) invocationOnMock.getArguments()[2];
            listener.onResponse((TransferFileSnapshot) invocationOnMock.getArguments()[0]);
            return null;
        }).when(transferService).uploadBlobAsync(any(TransferFileSnapshot.class), any(BlobPath.class), any(ActionListener.class));

        TranslogTransferManager translogTransferManager = new TranslogTransferManager(
            transferService,
            remoteBaseTransferPath,
            new FileTransferListener() {
                @Override
                public void onSuccess(TransferFileSnapshot fileSnapshot) {
                    fileTransferSucceeded.incrementAndGet();
                }

                @Override
                public void onFailure(TransferFileSnapshot fileSnapshot, Exception e) {
                    fileTransferFailed.incrementAndGet();
                }
            },
            r -> r
        );

        assertTrue(translogTransferManager.transferSnapshot(createTransferSnapshot(), new TranslogTransferListener() {
            @Override
            public void onUploadComplete(TransferSnapshot transferSnapshot) {
                translogTransferSucceeded.incrementAndGet();
            }

            @Override
            public void onUploadFailed(TransferSnapshot transferSnapshot, Exception ex) {
                translogTransferFailed.incrementAndGet();
            }
        }));
        assertEquals(4, fileTransferSucceeded.get());
        assertEquals(0, fileTransferFailed.get());
        assertEquals(1, translogTransferSucceeded.get());
        assertEquals(0, translogTransferFailed.get());
    }

    private TransferSnapshot createTransferSnapshot() {
        return new TransferSnapshot() {
            @Override
            public Set<TransferFileSnapshot> getCheckpointFileSnapshots() {
                try {
                    return Set.of(
                        new CheckpointFileSnapshot(
                            primaryTerm,
                            generation,
                            minTranslogGeneration,
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.CHECKPOINT_SUFFIX)
                        ),
                        new CheckpointFileSnapshot(
                            primaryTerm,
                            generation,
                            minTranslogGeneration,
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.CHECKPOINT_SUFFIX)
                        )
                    );
                } catch (IOException e) {
                    throw new AssertionError("Failed to create temp file", e);
                }
            }

            @Override
            public Set<TransferFileSnapshot> getTranslogFileSnapshots() {
                try {
                    return Set.of(
                        new TranslogFileSnapshot(
                            primaryTerm,
                            generation,
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + generation, Translog.TRANSLOG_FILE_SUFFIX)
                        ),
                        new TranslogFileSnapshot(
                            primaryTerm,
                            generation - 1,
                            createTempFile(Translog.TRANSLOG_FILE_PREFIX + (generation - 1), Translog.TRANSLOG_FILE_SUFFIX)
                        )
                    );
                } catch (IOException e) {
                    throw new AssertionError("Failed to create temp file", e);
                }
            }

            @Override
            public TranslogTransferMetadata getTranslogTransferMetadata() {
                return new TranslogTransferMetadata(primaryTerm, generation, minTranslogGeneration, randomInt(5));
            }
        };
    }
}
